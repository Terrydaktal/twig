use chrono::{DateTime, Datelike, Local};
use clap::{Parser, ValueEnum};
use jemallocator::Jemalloc;
use jwalk::WalkDir;
use lscolors::{LsColors, Style};
use std::collections::{HashMap, HashSet};
use std::ffi::{OsStr, OsString};
use std::fs;
use std::io::{self, Write};
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use users::{get_group_by_gid, get_user_by_uid};

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const HYPERLINK_MAX_FILES: usize = 1000;

#[derive(ValueEnum, Clone, Debug, Copy, PartialEq)]
enum SortBy {
    Name,
    Type,
    Date,
    Size,
}

#[derive(Parser)]
#[command(name = "twig")]
#[command(
    about = "A faster, more functional, more fine grained, more comprehensive, more user friendly and more modular eza clone",
    long_about = None
)]
struct Cli {
    /// List all files, including hidden ones
    #[arg(short, long)]
    all: bool,

    /// List all files, but exclude . and ..
    #[arg(short = 'A', long)]
    almost_all: bool,

    /// Use a long listing format (short for -psot)
    #[arg(short, long)]
    long: bool,

    /// Show permissions
    #[arg(short, long)]
    permissions: bool,

    /// Show size: files use logical bytes; dirs use allocated blocks
    #[arg(short, long)]
    size: bool,

    /// Show owner user
    #[arg(short, long)]
    owner: bool,

    /// Show group
    #[arg(short, long)]
    group: bool,

    /// Show modification time
    #[arg(short = 't', long = "modified")]
    modified: bool,

    /// Append indicator (one of /=>@|) to entries
    #[arg(short = 'F', long)]
    classify: bool,

    /// Which field to sort by
    #[arg(long, value_enum, default_value = "type")]
    sort: SortBy,

    /// Reverse output order
    #[arg(short, long)]
    reverse: bool,

    /// Render names as terminal hyperlinks
    #[arg(long)]
    hyperlink: bool,

    /// Show Git staged/unstaged status as a two-character column
    #[arg(long)]
    git: bool,

    /// Show Git repository status for directories that are repository roots
    #[arg(long = "git-repos")]
    git_repos: bool,

    /// Show true size: files use allocated blocks; dirs use recursive allocated blocks
    #[arg(short = 'S', long = "true-size")]
    true_size: bool,

    /// Disable hardlink deduplication when calculating true sizes
    #[arg(
        short = 'H',
        long = "no-dedupe-hardlinks",
        action = clap::ArgAction::SetFalse,
        default_value_t = true,
        requires = "true_size"
    )]
    dedupe_hardlinks: bool,

    /// Cache shown output paths to /tmp/fzf-history-$USER/universal-last-{dirs,files}-<fish pid>
    #[arg(long)]
    cache_raw: bool,

    /// The path to list
    #[arg(default_value = ".")]
    path: String,
}

struct Context {
    lscolors: LsColors,
    classify: bool,
    size_requested: bool,
    show_perms: bool,
    show_size: bool,
    show_owner: bool,
    show_group: bool,
    show_time: bool,
    hyperlink: bool,
    show_git: bool,
    show_git_repos: bool,
    true_size: bool,
    sort_by: SortBy,
}

struct EntryInfo {
    display_name: String,
    actual_path: PathBuf,
    metadata: fs::Metadata,
    is_symlink: bool,
    is_dir: bool,
    is_target_dir: bool,
    is_hidden: bool,
    size_str: String,
    user_str: String,
    group_str: String,
    time_str: String,
    final_size: u64,
    symlink_target: Option<PathBuf>,
    broken_symlink: bool,
    git_status: Option<(char, char)>,
    repo_status: Option<char>,
}

fn main() {
    let cli = Cli::parse();
    let lscolors = LsColors::from_env().unwrap_or_default();
    let show_hidden = cli.all || cli.almost_all;

    let mut ctx = Context {
        lscolors,
        classify: cli.classify,
        size_requested: cli.size || cli.long || cli.true_size,
        show_perms: cli.permissions || cli.long,
        show_size: cli.size || cli.long || cli.true_size,
        show_owner: cli.owner || cli.long,
        show_group: cli.group,
        show_time: cli.modified || cli.long,
        hyperlink: cli.hyperlink,
        show_git: cli.git,
        show_git_repos: cli.git_repos,
        true_size: cli.true_size,
        sort_by: cli.sort,
    };

    let is_detailed = ctx.show_perms
        || ctx.show_size
        || ctx.show_owner
        || ctx.show_group
        || ctx.show_time
        || ctx.show_git
        || ctx.show_git_repos;
    let mut entries = Vec::new();
    let recursive_sizes = if cli.true_size {
        collect_recursive_sizes(Path::new(&cli.path), show_hidden, cli.dedupe_hardlinks)
    } else {
        HashMap::new()
    };

    if cli.all {
        if let Ok(m) = fs::symlink_metadata(&cli.path) {
            entries.push(create_entry_info(
                ".",
                PathBuf::from(&cli.path),
                m,
                &ctx,
                &recursive_sizes,
            ));
        }
        let parent_path = if cli.path == "." {
            "..".to_string()
        } else {
            format!("{}/..", cli.path)
        };
        if let Ok(m) = fs::symlink_metadata(&parent_path) {
            entries.push(create_entry_info(
                "..",
                PathBuf::from(&parent_path),
                m,
                &ctx,
                &recursive_sizes,
            ));
        }
    }

    let walk_dir = WalkDir::new(&cli.path)
        .max_depth(1)
        .skip_hidden(!show_hidden);

    for entry in walk_dir {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        if entry.depth == 0 {
            continue;
        }
        let metadata = match entry.metadata() {
            Ok(m) => m,
            Err(_) => continue,
        };
        let file_name = entry.file_name().to_string_lossy().to_string();
        entries.push(create_entry_info(
            &file_name,
            entry.path(),
            metadata,
            &ctx,
            &recursive_sizes,
        ));
    }

    if entries.is_empty() {
        if cli.cache_raw {
            let _ = write_cache_raw_paths(&[], &[]);
        }
        return;
    }

    if cli.hyperlink {
        let shown_file_count = entries
            .iter()
            .filter(|entry| {
                let is_dirish = if entry.is_symlink {
                    entry.is_target_dir
                } else {
                    entry.is_dir
                };
                !is_dirish
            })
            .count();
        if shown_file_count > HYPERLINK_MAX_FILES {
            ctx.hyperlink = false;
        }
    }

    entries.sort_by(|a, b| {
        match ctx.sort_by {
            SortBy::Size => {
                if a.final_size != b.final_size {
                    return b.final_size.cmp(&a.final_size);
                }
                let a_name = a.display_name.trim_start_matches('.').to_lowercase();
                let b_name = b.display_name.trim_start_matches('.').to_lowercase();
                return a_name.cmp(&b_name);
            }
            SortBy::Date => {
                let a_time = a.metadata.mtime();
                let b_time = b.metadata.mtime();
                if a_time != b_time {
                    return b_time.cmp(&a_time);
                }
            }
            SortBy::Type => {
                let a_rank = get_custom_type_rank(a);
                let b_rank = get_custom_type_rank(b);
                if a_rank != b_rank {
                    return a_rank.cmp(&b_rank);
                }
            }
            SortBy::Name => {}
        }
        if a.is_hidden != b.is_hidden {
            return b.is_hidden.cmp(&a.is_hidden);
        }
        a.display_name.cmp(&b.display_name)
    });
    if cli.reverse {
        entries.reverse();
    }

    if ctx.show_git || ctx.show_git_repos {
        populate_git_columns(
            Path::new(&cli.path),
            &mut entries,
            ctx.show_git,
            ctx.show_git_repos,
        );
    }

    if cli.cache_raw {
        let (shown_dir_paths, shown_file_paths) = collect_output_paths(&entries);
        if let Err(err) = write_cache_raw_paths(&shown_dir_paths, &shown_file_paths) {
            eprintln!("failed to write --cache-raw files: {}", err);
        }
    }

    let output = if is_detailed {
        print_detailed_list(&entries, &ctx)
    } else {
        let mut out = String::new();
        for entry in &entries {
            out.push_str(&get_styled_name(
                &entry.display_name,
                &entry.actual_path,
                &entry.metadata,
                &ctx,
            ));
            out.push_str("  ");
        }
        out.push('\n');
        out
    };

    let mut stdout = io::stdout().lock();
    let _ = stdout.write_all(output.as_bytes());
}

fn collect_output_paths(entries: &[EntryInfo]) -> (Vec<PathBuf>, Vec<PathBuf>) {
    let mut dir_paths = Vec::new();
    let mut file_paths = Vec::new();
    for entry in entries {
        let full_path = to_full_path(&entry.actual_path);
        let is_dir = if entry.is_symlink {
            entry.is_target_dir
        } else {
            entry.is_dir
        };
        if is_dir {
            dir_paths.push(full_path);
        } else {
            file_paths.push(full_path);
        }
    }
    (dir_paths, file_paths)
}

fn populate_git_columns(
    listing_path: &Path,
    entries: &mut [EntryInfo],
    show_git: bool,
    show_git_repos: bool,
) {
    if show_git {
        let status_map = collect_git_statuses_for_listing(listing_path).unwrap_or_default();
        for entry in entries.iter_mut() {
            let status = status_map
                .get(&entry.display_name)
                .copied()
                .unwrap_or(('-', '-'));
            entry.git_status = Some(status);
        }
    }

    if show_git_repos {
        for entry in entries.iter_mut() {
            let is_dirish = if entry.is_symlink {
                entry.is_target_dir
            } else {
                entry.is_dir
            };
            if !is_dirish {
                continue;
            }
            entry.repo_status = git_repo_root_status(&entry.actual_path);
        }
    }
}

fn collect_git_statuses_for_listing(listing_path: &Path) -> Option<HashMap<String, (char, char)>> {
    let listing_abs = fs::canonicalize(listing_path).unwrap_or_else(|_| to_full_path(listing_path));
    let repo_root = git_repo_root(&listing_abs)?;
    let rel_prefix = listing_abs
        .strip_prefix(&repo_root)
        .ok()
        .unwrap_or(Path::new(""));

    let output = Command::new("git")
        .arg("-C")
        .arg(&listing_abs)
        .arg("-c")
        .arg("core.quotepath=false")
        .args([
            "status",
            "--porcelain=v1",
            "--ignored=matching",
            "--untracked-files=all",
        ])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut status_map: HashMap<String, (char, char)> = HashMap::new();
    for line in stdout.lines() {
        if line.len() < 3 {
            continue;
        }

        let xy = &line[0..2];
        let mut path_part = line[3..].trim_start();
        if let Some((_, new_path)) = path_part.rsplit_once(" -> ") {
            path_part = new_path;
        }

        let (staged_raw, unstaged_raw) = parse_git_status_pair(xy);

        let status_rel = Path::new(path_part);
        let relevant = if rel_prefix.as_os_str().is_empty() {
            status_rel
        } else {
            match status_rel.strip_prefix(rel_prefix) {
                Ok(p) => p,
                Err(_) => continue,
            }
        };
        if relevant.as_os_str().is_empty() {
            continue;
        }

        let Some(top_component) = relevant.components().next() else {
            continue;
        };
        let top_name = top_component.as_os_str().to_string_lossy().into_owned();
        let staged = git_status_symbol(staged_raw);
        let unstaged = git_status_symbol(unstaged_raw);
        let current = status_map.entry(top_name).or_insert(('-', '-'));
        current.0 = pick_stronger_git_status(current.0, staged);
        current.1 = pick_stronger_git_status(current.1, unstaged);
    }

    Some(status_map)
}

fn git_repo_root(path: &Path) -> Option<PathBuf> {
    let output = Command::new("git")
        .arg("-C")
        .arg(path)
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let root = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if root.is_empty() {
        return None;
    }
    Some(PathBuf::from(root))
}

fn git_repo_root_status(path: &Path) -> Option<char> {
    let abs_path = fs::canonicalize(path).unwrap_or_else(|_| to_full_path(path));
    if !abs_path.join(".git").exists() {
        return None;
    }

    let Some(repo_root) = git_repo_root(&abs_path) else {
        return Some('~');
    };
    let repo_root_abs = fs::canonicalize(&repo_root).unwrap_or(repo_root);
    if repo_root_abs != abs_path {
        return None;
    }

    let output = Command::new("git")
        .arg("-C")
        .arg(&abs_path)
        .args(["status", "--porcelain=v1", "--untracked-files=normal"])
        .output();
    let Ok(output) = output else {
        return Some('~');
    };
    if !output.status.success() {
        return Some('~');
    }
    if output.stdout.is_empty() {
        Some('|')
    } else {
        Some('+')
    }
}

fn git_status_symbol(raw: char) -> char {
    match raw {
        ' ' => '-',
        'M' => 'M',
        'A' => 'A',
        '?' => 'N',
        'D' => 'D',
        'R' => 'R',
        'T' => 'T',
        '!' => 'I',
        'U' => 'U',
        'C' => 'M',
        _ => '-',
    }
}

fn parse_git_status_pair(xy: &str) -> (char, char) {
    match xy {
        "??" => (' ', '?'),
        "!!" => (' ', '!'),
        // Any unmerged state should read as conflicted in both columns.
        "DD" | "AU" | "UD" | "UA" | "DU" | "AA" | "UU" => ('U', 'U'),
        _ => {
            let mut chars = xy.chars();
            let x = chars.next().unwrap_or(' ');
            let y = chars.next().unwrap_or(' ');
            (x, y)
        }
    }
}

fn git_status_rank(symbol: char) -> u8 {
    match symbol {
        'U' => 8,
        'D' => 7,
        'R' => 6,
        'T' => 5,
        'M' => 4,
        'A' | 'N' => 3,
        'I' => 2,
        '-' => 1,
        _ => 0,
    }
}

fn pick_stronger_git_status(current: char, next: char) -> char {
    if git_status_rank(next) >= git_status_rank(current) {
        next
    } else {
        current
    }
}

fn git_symbol_style(symbol: char) -> nu_ansi_term::Style {
    match symbol {
        'N' | 'A' => nu_ansi_term::Color::Green.normal(),
        'M' | 'R' | 'T' => nu_ansi_term::Color::Yellow.normal(),
        'D' | 'U' => nu_ansi_term::Color::Red.normal(),
        '-' | 'I' => nu_ansi_term::Style::default().dimmed(),
        _ => nu_ansi_term::Style::default(),
    }
}

fn git_repo_status_style(symbol: char) -> nu_ansi_term::Style {
    match symbol {
        '+' => nu_ansi_term::Color::Red.normal(),
        '|' => nu_ansi_term::Color::Green.normal(),
        '~' => nu_ansi_term::Color::Yellow.normal(),
        _ => nu_ansi_term::Style::default().dimmed(),
    }
}

fn get_custom_type_rank(e: &EntryInfo) -> u8 {
    let is_dir = if e.is_symlink {
        e.is_target_dir
    } else {
        e.is_dir
    };
    if e.is_symlink && is_dir {
        0
    } else if is_dir {
        1
    } else if e.is_symlink {
        2
    } else {
        3
    }
}

fn on_disk_size(metadata: &fs::Metadata) -> u64 {
    metadata.blocks() * 512
}

fn collect_recursive_sizes(
    base_path: &Path,
    show_hidden: bool,
    dedupe_hardlinks: bool,
) -> HashMap<OsString, u64> {
    let canonical_base = fs::canonicalize(base_path).unwrap_or_else(|_| base_path.to_path_buf());
    let scan_root = canonical_base.clone();
    let dir_local_sums = Arc::new(Mutex::new(HashMap::<PathBuf, u64>::new()));
    let shared_sums = Arc::clone(&dir_local_sums);
    let seen_inodes = if dedupe_hardlinks {
        Some(Arc::new(Mutex::new(HashSet::<(u64, u64)>::new())))
    } else {
        None
    };
    let shared_seen = seen_inodes.as_ref().map(Arc::clone);

    WalkDir::new(&canonical_base)
        .skip_hidden(!show_hidden)
        .process_read_dir(move |_depth, path, _state, children| {
            let current_path = if path.is_absolute() {
                path.to_path_buf()
            } else {
                scan_root.join(path)
            };

            let mut local_sum = 0u64;
            let mut hardlink_candidates: Vec<(u64, u64, u64)> = Vec::new();

            for child in children.iter_mut().filter_map(|e| e.as_mut().ok()) {
                if let Ok(metadata) = child.metadata() {
                    if shared_seen.is_none() || metadata.is_dir() || metadata.nlink() <= 1 {
                        local_sum += on_disk_size(&metadata);
                    } else {
                        hardlink_candidates.push((
                            metadata.dev(),
                            metadata.ino(),
                            on_disk_size(&metadata),
                        ));
                    }
                }
            }

            if let Some(ref seen) = shared_seen {
                if !hardlink_candidates.is_empty() {
                    if let Ok(mut set) = seen.lock() {
                        for (dev, ino, size) in hardlink_candidates {
                            if set.insert((dev, ino)) {
                                local_sum += size;
                            }
                        }
                    }
                }
            }

            if let Ok(mut sums) = shared_sums.lock() {
                sums.insert(current_path, local_sum);
            }
        })
        .into_iter()
        .for_each(|_| {});

    let mut aggregated = match Arc::try_unwrap(dir_local_sums) {
        Ok(mutex) => mutex.into_inner().unwrap_or_default(),
        Err(shared) => shared.lock().map(|m| m.clone()).unwrap_or_default(),
    };

    aggregated.entry(canonical_base.clone()).or_insert(0);
    let mut paths: Vec<PathBuf> = aggregated.keys().cloned().collect();
    paths.sort_unstable_by_key(|p| std::cmp::Reverse(p.components().count()));

    for path in paths {
        if path == canonical_base {
            continue;
        }

        if let Some(parent) = path.parent() {
            if !(parent == canonical_base || parent.starts_with(&canonical_base)) {
                continue;
            }

            let value = aggregated.get(&path).copied().unwrap_or(0);
            *aggregated.entry(parent.to_path_buf()).or_insert(0) += value;
        }
    }

    let mut recursive_sizes: HashMap<OsString, u64> = HashMap::new();
    for (path, descendant_sum) in aggregated {
        if path.parent() != Some(canonical_base.as_path()) {
            continue;
        }

        let metadata = match fs::symlink_metadata(&path) {
            Ok(m) => m,
            Err(_) => continue,
        };

        if !metadata.is_dir() {
            continue;
        }

        if let Some(name) = path.file_name() {
            recursive_sizes.insert(
                name.to_os_string(),
                on_disk_size(&metadata) + descendant_sum,
            );
        }
    }

    recursive_sizes
}

fn create_entry_info(
    display_name: &str,
    actual_path: PathBuf,
    metadata: fs::Metadata,
    ctx: &Context,
    recursive_sizes: &HashMap<OsString, u64>,
) -> EntryInfo {
    let is_symlink = metadata.file_type().is_symlink();
    let is_dir = metadata.is_dir();
    let is_hidden = display_name.starts_with('.') && display_name != "." && display_name != "..";

    let mut is_target_dir = false;
    let mut symlink_target = None;
    let mut broken_symlink = false;
    if is_symlink {
        symlink_target = fs::read_link(&actual_path).ok();
        let target_meta = fs::metadata(&actual_path).ok();
        is_target_dir = target_meta.as_ref().map(|m| m.is_dir()).unwrap_or(false);
        broken_symlink = target_meta.is_none();
    }

    let final_size = if ctx.true_size {
        recursive_sizes
            .get(OsStr::new(display_name))
            .copied()
            .unwrap_or_else(|| on_disk_size(&metadata))
    } else if is_dir {
        on_disk_size(&metadata)
    } else {
        metadata.len()
    };

    let size_str = if (is_dir || (is_symlink && is_target_dir))
        && !ctx.size_requested
        && ctx.sort_by != SortBy::Size
        && !ctx.true_size
    {
        "-".to_string()
    } else {
        format_size(final_size)
    };

    let user_str = if ctx.show_owner {
        get_user_by_uid(metadata.uid())
            .map(|u| u.name().to_string_lossy().into_owned())
            .unwrap_or_else(|| metadata.uid().to_string())
    } else {
        String::new()
    };
    let group_str = if ctx.show_group {
        get_group_by_gid(metadata.gid())
            .map(|g| g.name().to_string_lossy().into_owned())
            .unwrap_or_else(|| metadata.gid().to_string())
    } else {
        String::new()
    };

    let time_str = if ctx.show_time {
        let mtime = metadata.mtime();
        let dt: DateTime<Local> = DateTime::from_timestamp(mtime, 0)
            .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap())
            .with_timezone(&Local);
        let now = Local::now();
        if now.year() == dt.year() && (now.timestamp() - dt.timestamp()).abs() < 15552000 {
            dt.format("%e %b %H:%M").to_string()
        } else {
            dt.format("%e %b  %Y").to_string()
        }
    } else {
        String::new()
    };

    EntryInfo {
        display_name: display_name.to_string(),
        actual_path,
        metadata,
        is_symlink,
        is_dir,
        is_target_dir,
        is_hidden,
        size_str,
        user_str,
        group_str,
        time_str,
        final_size,
        symlink_target,
        broken_symlink,
        git_status: None,
        repo_status: None,
    }
}

fn print_detailed_list(entries: &[EntryInfo], ctx: &Context) -> String {
    let (mut max_size, mut max_user, mut max_group, mut max_time) = (0, 0, 0, 0);
    for e in entries {
        max_size = max_size.max(e.size_str.len());
        max_user = max_user.max(e.user_str.len());
        max_group = max_group.max(e.group_str.len());
        max_time = max_time.max(e.time_str.len());
    }

    let mut out = String::new();
    for e in entries {
        let mut row = String::new();
        if ctx.show_perms {
            let ft = if e.is_dir {
                paint_text_with_lscolors("d", &e.actual_path, &e.metadata, ctx)
            } else if e.is_symlink {
                nu_ansi_term::Color::LightCyan.bold().paint("l").to_string()
            } else {
                nu_ansi_term::Color::White.bold().paint("-").to_string()
            };
            row.push_str(&ft);
            row.push_str(&format_permissions(e.metadata.permissions().mode()));
        }
        if ctx.show_size {
            if !row.is_empty() {
                row.push(' ');
            }
            row.push_str(
                &nu_ansi_term::Color::LightCyan
                    .bold()
                    .paint(format!("{:>width$}", e.size_str, width = max_size))
                    .to_string(),
            );
        }
        if ctx.show_owner {
            if !row.is_empty() {
                row.push(' ');
            }
            row.push_str(&format!("{:<width$}", e.user_str, width = max_user));
        }
        if ctx.show_group {
            if !row.is_empty() {
                row.push(' ');
            }
            row.push_str(&format!("{:<width$}", e.group_str, width = max_group));
        }
        if ctx.show_time {
            if !row.is_empty() {
                row.push(' ');
            }
            row.push_str(
                &nu_ansi_term::Style::default()
                    .dimmed()
                    .paint(format!("{:<width$}", e.time_str, width = max_time))
                    .to_string(),
            );
        }
        if ctx.show_git {
            if !row.is_empty() {
                row.push(' ');
            }
            let (staged, unstaged) = e.git_status.unwrap_or(('-', '-'));
            row.push_str(
                &git_symbol_style(staged)
                    .paint(staged.to_string())
                    .to_string(),
            );
            row.push_str(
                &git_symbol_style(unstaged)
                    .paint(unstaged.to_string())
                    .to_string(),
            );
        }
        if ctx.show_git_repos {
            if !row.is_empty() {
                row.push(' ');
            }
            let repo_status = e.repo_status.unwrap_or(' ');
            row.push_str(
                &git_repo_status_style(repo_status)
                    .paint(repo_status.to_string())
                    .to_string(),
            );
        }
        if !row.is_empty() {
            row.push(' ');
        }
        if e.is_symlink && e.broken_symlink {
            let mut broken_text = get_display_name_text(&e.display_name, &e.metadata, ctx);
            if let Some(target) = e.symlink_target.as_ref() {
                broken_text.push_str(" -> ");
                broken_text.push_str(&target.to_string_lossy());
            }
            row.push_str(&highlight_broken_symlink_text(&broken_text));
        } else {
            row.push_str(&get_styled_name(
                &e.display_name,
                &e.actual_path,
                &e.metadata,
                ctx,
            ));
            if let Some(target) = e.symlink_target.as_ref() {
                row.push_str(" -> ");
                row.push_str(&get_symlink_target_display(&e.actual_path, target, ctx));
            }
        }
        out.push_str(&row);
        out.push('\n');
    }

    out
}

fn get_styled_name(
    display_name: &str,
    actual_path: &Path,
    metadata: &fs::Metadata,
    ctx: &Context,
) -> String {
    let name = get_display_name_text(display_name, metadata, ctx);
    if metadata.file_type().is_symlink() && fs::metadata(actual_path).is_err() {
        return highlight_broken_symlink_text(&name);
    }
    let painted = paint_text_with_lscolors(&name, actual_path, metadata, ctx);
    if ctx.hyperlink {
        return hyperlink_path(actual_path, &painted);
    }
    painted
}

fn paint_text_with_lscolors(
    text: &str,
    path: &Path,
    metadata: &fs::Metadata,
    ctx: &Context,
) -> String {
    match ctx
        .lscolors
        .style_for_path_with_metadata(path, Some(metadata))
    {
        Some(style) => {
            let ansi_style = Style::to_nu_ansi_term_style(style);
            if ansi_style == nu_ansi_term::Style::default() {
                text.to_string()
            } else {
                ansi_style.paint(text).to_string()
            }
        }
        None => text.to_string(),
    }
}

fn get_display_name_text(display_name: &str, metadata: &fs::Metadata, ctx: &Context) -> String {
    let mut name = display_name.to_string();
    if ctx.classify {
        if metadata.file_type().is_symlink() {
            name.push('@');
        } else if metadata.is_dir() {
            name.push('/');
        } else if metadata.permissions().mode() & 0o111 != 0 {
            name.push('*');
        }
    }
    name
}

fn highlight_broken_symlink_text(text: &str) -> String {
    format!("\x1b[48;2;255;0;0m\x1b[38;2;255;255;255m{}\x1b[0m", text)
}

fn hyperlink_path(path: &Path, text: &str) -> String {
    if let Some(abs) = to_absolute_path(path) {
        return format!(
            "\x1b]8;;file://{}\x1b\\{}\x1b]8;;\x1b\\",
            abs.to_string_lossy(),
            text
        );
    }
    text.to_string()
}

fn to_absolute_path(path: &Path) -> Option<PathBuf> {
    if path.is_absolute() {
        Some(path.to_path_buf())
    } else {
        std::env::current_dir().ok().map(|cwd| cwd.join(path))
    }
}

fn to_full_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    }
}

fn write_path_list(cache_path: &Path, paths: &[PathBuf]) -> io::Result<()> {
    let mut output = String::new();
    for path in paths {
        output.push_str(&path.to_string_lossy());
        output.push('\n');
    }
    fs::write(cache_path, output)
}

fn cache_pid_suffix() -> u32 {
    if let Some(value) = std::env::var_os("fish_pid") {
        if let Some(text) = value.to_str() {
            if let Ok(pid) = text.parse::<u32>() {
                return pid;
            }
        }
    }

    if let Ok(stat) = fs::read_to_string("/proc/self/stat") {
        if let Some((_, after_comm)) = stat.rsplit_once(") ") {
            let mut fields = after_comm.split_whitespace();
            let _state = fields.next();
            if let Some(ppid_field) = fields.next() {
                if let Ok(ppid) = ppid_field.parse::<u32>() {
                    return ppid;
                }
            }
        }
    }

    std::process::id()
}

fn write_cache_raw_paths(dir_paths: &[PathBuf], file_paths: &[PathBuf]) -> io::Result<()> {
    let user = std::env::var("USER").unwrap_or_else(|_| "unknown".to_string());
    let cache_dir = PathBuf::from("/tmp").join(format!("fzf-history-{}", user));
    let pid = cache_pid_suffix();
    fs::create_dir_all(&cache_dir)?;
    write_path_list(
        &cache_dir.join(format!("universal-last-dirs-{}", pid)),
        dir_paths,
    )?;
    write_path_list(
        &cache_dir.join(format!("universal-last-files-{}", pid)),
        file_paths,
    )
}

fn get_symlink_target_display(link_path: &Path, target: &Path, ctx: &Context) -> String {
    let mut display_text = target.to_string_lossy().into_owned();
    let resolved_target = if target.is_absolute() {
        Some(target.to_path_buf())
    } else {
        link_path.parent().map(|p| p.join(target))
    };

    let painted = if let Some(path) = resolved_target.as_deref() {
        let target_is_dir = fs::metadata(path).map(|m| m.is_dir()).unwrap_or(false);
        if ctx.classify && target_is_dir && !display_text.ends_with('/') {
            display_text.push('/');
        }

        let (split_text, trailing_slash) = if display_text.ends_with('/') && display_text.len() > 1
        {
            (&display_text[..display_text.len() - 1], "/")
        } else {
            (display_text.as_str(), "")
        };

        let (prefix, basename) = match split_text.rfind('/') {
            Some(idx) if idx + 1 < split_text.len() => {
                (&split_text[..idx + 1], &split_text[idx + 1..])
            }
            _ => ("", split_text),
        };
        let basename_with_suffix = format!("{}{}", basename, trailing_slash);
        let target_metadata = fs::symlink_metadata(path).ok();
        let target_style = ctx
            .lscolors
            .style_for_path_with_metadata(path, target_metadata.as_ref())
            .or_else(|| ctx.lscolors.style_for_path(path));
        let styled_basename = match target_style {
            Some(style) => {
                let ansi_style = Style::to_nu_ansi_term_style(style);
                if ansi_style == nu_ansi_term::Style::default() {
                    basename_with_suffix
                } else {
                    ansi_style.paint(basename_with_suffix).to_string()
                }
            }
            None => basename_with_suffix,
        };
        if prefix.is_empty() {
            styled_basename
        } else {
            format!("\x1b[38;2;255;255;255m{}\x1b[0m{}", prefix, styled_basename)
        }
    } else {
        display_text
    };

    if ctx.hyperlink {
        if let Some(path) = resolved_target {
            return hyperlink_path(&path, &painted);
        }
    }
    painted
}

fn format_permissions(mode: u32) -> String {
    let p = [
        (0o400, "r", nu_ansi_term::Color::LightYellow.bold()),
        (0o200, "w", nu_ansi_term::Color::LightRed.bold()),
        (0o100, "x", nu_ansi_term::Color::LightGreen.bold()),
        // Keep group/other permissions in the same color families, but dimmer.
        (0o040, "r", nu_ansi_term::Color::Rgb(180, 180, 120).normal()),
        (0o020, "w", nu_ansi_term::Color::Rgb(190, 120, 120).normal()),
        (0o010, "x", nu_ansi_term::Color::Rgb(120, 180, 120).normal()),
        (0o004, "r", nu_ansi_term::Color::Rgb(180, 180, 120).normal()),
        (0o002, "w", nu_ansi_term::Color::Rgb(190, 120, 120).normal()),
        (0o001, "x", nu_ansi_term::Color::Rgb(120, 180, 120).normal()),
    ];
    let mut s = String::new();
    let dash = nu_ansi_term::Color::Fixed(236).paint("-");
    for (mask, c, style) in p.iter() {
        if mode & mask != 0 {
            s.push_str(&style.paint(*c).to_string());
        } else {
            s.push_str(&dash.to_string());
        }
    }
    s
}

fn format_size(size: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    if size >= GB {
        format!("{:.1}G", size as f64 / GB as f64)
    } else if size >= MB {
        format!("{:.1}M", size as f64 / MB as f64)
    } else if size >= KB {
        format!("{:.1}K", size as f64 / KB as f64)
    } else {
        size.to_string()
    }
}
