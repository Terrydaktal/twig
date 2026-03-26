use chrono::{DateTime, Datelike, Local};
use clap::{Parser, ValueEnum};
use jemallocator::Jemalloc;
use jwalk::WalkDir;
use lscolors::{LsColors, Style};
use std::collections::{HashMap, HashSet};
use std::ffi::{OsStr, OsString};
use std::fs;
use std::io::{self, IsTerminal, Write};
use std::os::unix::fs::FileTypeExt;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};
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
    #[value(name = "dircount")]
    DirCount,
    #[value(name = "filecount")]
    FileCount,
}

#[derive(ValueEnum, Clone, Debug, Copy, PartialEq, Eq)]
enum ColorMode {
    Always,
    Never,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DetailColumn {
    Perms,
    SizeLogical,
    DirCount,
    FileCount,
    Owner,
    Time,
    Group,
    SizeTrue,
    Git,
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

    /// Use a long listing format (short for -Lptos --show-targets)
    #[arg(short, long)]
    long: bool,

    /// List one entry per line
    #[arg(short = 'L', long = "list")]
    list: bool,

    /// Show permissions
    #[arg(short, long)]
    permissions: bool,

    /// Show size: files use logical bytes; dirs use allocated blocks
    #[arg(short, long)]
    size: bool,

    /// Show recursive directory and file counts for directories
    #[arg(short = 'c', long = "counts")]
    counts: bool,

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

    /// Control colorized output
    #[arg(long, value_enum, default_value = "always")]
    color: ColorMode,

    /// Render names as terminal hyperlinks
    #[arg(short = 'U', long)]
    hyperlink: bool,

    /// Show symlink targets
    #[arg(short = 'x', long = "show-targets")]
    show_targets: bool,

    /// Show absolute paths in output
    #[arg(short = 'X', long = "absolute")]
    absolute: bool,

    /// Dereference symlink targets for size/time calculations
    #[arg(short = 'd', long = "dereference")]
    dereference: bool,

    /// Show Git columns: file status in repos and repo-root status for listed repo dirs
    #[arg(short = 'G', long)]
    git: bool,

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

    /// Show a header row for list/detailed output
    #[arg(long)]
    header: bool,

    /// The path to list
    #[arg(default_value = ".")]
    path: String,
}

struct Context {
    lscolors: LsColors,
    color_enabled: bool,
    classify: bool,
    show_perms: bool,
    show_size_logical: bool,
    show_size_true: bool,
    show_counts: bool,
    show_owner: bool,
    show_group: bool,
    show_time: bool,
    hyperlink: bool,
    show_targets: bool,
    absolute: bool,
    dereference: bool,
    show_git: bool,
    show_git_repos: bool,
    show_hidden: bool,
    dedupe_hardlinks: bool,
    reverse: bool,
    header: bool,
    column_preference: Vec<DetailColumn>,
    sort_by: SortBy,
}

struct EntryInfo {
    display_name: String,
    render_name: String,
    actual_path: PathBuf,
    metadata: fs::Metadata,
    is_symlink: bool,
    is_dir: bool,
    is_target_dir: bool,
    is_hidden: bool,
    logical_size_str: String,
    true_size_str: String,
    dir_count_str: String,
    file_count_str: String,
    user_str: String,
    group_str: String,
    time_str: String,
    final_size: u64,
    dir_count: u64,
    file_count: u64,
    sort_mtime: i64,
    symlink_target: Option<PathBuf>,
    broken_symlink: bool,
    git_status: Option<(char, char)>,
    repo_status: Option<char>,
}

fn push_unique_column(columns: &mut Vec<DetailColumn>, column: DetailColumn) {
    if !columns.contains(&column) {
        columns.push(column);
    }
}

fn parse_detail_column_preference() -> Vec<DetailColumn> {
    let mut columns = Vec::new();
    let mut stop_parsing_flags = false;

    for arg in std::env::args_os().skip(1) {
        let arg = arg.to_string_lossy();
        if stop_parsing_flags {
            continue;
        }
        if arg == "--" {
            stop_parsing_flags = true;
            continue;
        }
        if let Some(long) = arg.strip_prefix("--") {
            match long {
                "permissions" => push_unique_column(&mut columns, DetailColumn::Perms),
                "size" => push_unique_column(&mut columns, DetailColumn::SizeLogical),
                "counts" => {
                    push_unique_column(&mut columns, DetailColumn::DirCount);
                    push_unique_column(&mut columns, DetailColumn::FileCount);
                }
                "owner" => push_unique_column(&mut columns, DetailColumn::Owner),
                "modified" => push_unique_column(&mut columns, DetailColumn::Time),
                "group" => push_unique_column(&mut columns, DetailColumn::Group),
                "true-size" => push_unique_column(&mut columns, DetailColumn::SizeTrue),
                "git" => push_unique_column(&mut columns, DetailColumn::Git),
                _ => {}
            }
            continue;
        }
        if let Some(shorts) = arg.strip_prefix('-') {
            if shorts.is_empty() {
                continue;
            }
            for ch in shorts.chars() {
                match ch {
                    'p' => push_unique_column(&mut columns, DetailColumn::Perms),
                    's' => push_unique_column(&mut columns, DetailColumn::SizeLogical),
                    'c' => {
                        push_unique_column(&mut columns, DetailColumn::DirCount);
                        push_unique_column(&mut columns, DetailColumn::FileCount);
                    }
                    'o' => push_unique_column(&mut columns, DetailColumn::Owner),
                    't' => push_unique_column(&mut columns, DetailColumn::Time),
                    'g' => push_unique_column(&mut columns, DetailColumn::Group),
                    'S' => push_unique_column(&mut columns, DetailColumn::SizeTrue),
                    'G' => push_unique_column(&mut columns, DetailColumn::Git),
                    _ => {}
                }
            }
        }
    }

    columns
}

fn is_detail_column_enabled(column: DetailColumn, ctx: &Context) -> bool {
    match column {
        DetailColumn::Perms => ctx.show_perms,
        DetailColumn::SizeLogical => ctx.show_size_logical,
        DetailColumn::DirCount | DetailColumn::FileCount => ctx.show_counts,
        DetailColumn::Owner => ctx.show_owner,
        DetailColumn::Time => ctx.show_time,
        DetailColumn::Group => ctx.show_group,
        DetailColumn::SizeTrue => ctx.show_size_true,
        DetailColumn::Git => ctx.show_git || ctx.show_git_repos,
    }
}

fn build_detail_columns(ctx: &Context) -> Vec<DetailColumn> {
    let mut columns = Vec::new();
    for column in &ctx.column_preference {
        if is_detail_column_enabled(*column, ctx) {
            push_unique_column(&mut columns, *column);
        }
    }
    for column in [
        DetailColumn::Perms,
        DetailColumn::SizeLogical,
        DetailColumn::DirCount,
        DetailColumn::FileCount,
        DetailColumn::Owner,
        DetailColumn::Time,
        DetailColumn::Group,
        DetailColumn::SizeTrue,
        DetailColumn::Git,
    ] {
        if is_detail_column_enabled(column, ctx) {
            push_unique_column(&mut columns, column);
        }
    }
    columns
}

fn main() {
    let cli = Cli::parse();
    let show_hidden = cli.all || cli.almost_all;

    let piped_output = !io::stdout().is_terminal();
    let color_enabled = matches!(cli.color, ColorMode::Always) && !piped_output;
    let classify_enabled = cli.classify && !piped_output;
    let hyperlink_enabled = cli.hyperlink && !piped_output;
    let cache_raw_enabled = cli.cache_raw && !piped_output;
    let lscolors = LsColors::from_env().unwrap_or_default();

    let mut ctx = Context {
        lscolors,
        color_enabled,
        classify: classify_enabled,
        show_perms: cli.permissions || cli.long,
        show_size_logical: cli.size || cli.long,
        show_size_true: cli.true_size,
        show_counts: cli.counts,
        show_owner: cli.owner || cli.long,
        show_group: cli.group,
        show_time: cli.modified || cli.long,
        hyperlink: hyperlink_enabled,
        show_targets: cli.show_targets || cli.long,
        absolute: cli.absolute,
        dereference: cli.dereference,
        show_git: false,
        show_git_repos: false,
        show_hidden,
        dedupe_hardlinks: cli.dedupe_hardlinks,
        reverse: cli.reverse,
        header: cli.header,
        column_preference: parse_detail_column_preference(),
        sort_by: cli.sort,
    };
    let mut entries = Vec::new();
    let need_counts = cli.counts || matches!(cli.sort, SortBy::DirCount | SortBy::FileCount);
    let (recursive_sizes, recursive_counts) = collect_recursive_stats(
        Path::new(&cli.path),
        show_hidden,
        cli.dedupe_hardlinks,
        cli.true_size,
        need_counts,
    );
    let mut user_cache: HashMap<u32, String> = HashMap::new();
    let mut group_cache: HashMap<u32, String> = HashMap::new();

    let input_path = Path::new(&cli.path);
    let input_meta = fs::symlink_metadata(input_path).ok();
    let input_is_dir = input_meta.as_ref().map(|m| m.is_dir()).unwrap_or(false);

    if cli.all && input_is_dir {
        if let Ok(m) = fs::symlink_metadata(&cli.path) {
            entries.push(create_entry_info(
                ".",
                PathBuf::from(&cli.path),
                m,
                &ctx,
                &recursive_sizes,
                &recursive_counts,
                &mut user_cache,
                &mut group_cache,
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
                &recursive_counts,
                &mut user_cache,
                &mut group_cache,
            ));
        }
    }

    if input_is_dir {
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
                &recursive_counts,
                &mut user_cache,
                &mut group_cache,
            ));
        }
    } else if let Some(metadata) = input_meta {
        let file_name = input_path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| cli.path.clone());
        entries.push(create_entry_info(
            &file_name,
            PathBuf::from(&cli.path),
            metadata,
            &ctx,
            &recursive_sizes,
            &recursive_counts,
            &mut user_cache,
            &mut group_cache,
        ));
    }

    if entries.is_empty() {
        if cache_raw_enabled {
            let _ = write_cache_raw_paths(&[], &[]);
        }
        return;
    }

    if ctx.hyperlink {
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
                let a_time = a.sort_mtime;
                let b_time = b.sort_mtime;
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
            SortBy::DirCount => {
                if a.dir_count != b.dir_count {
                    return b.dir_count.cmp(&a.dir_count);
                }
            }
            SortBy::FileCount => {
                if a.file_count != b.file_count {
                    return b.file_count.cmp(&a.file_count);
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

    if cli.git {
        let (show_git_status, show_repo_status) =
            populate_git_columns(Path::new(&cli.path), &mut entries);
        ctx.show_git = show_git_status;
        ctx.show_git_repos = show_repo_status;
    }

    if cache_raw_enabled {
        let (shown_dir_paths, shown_file_paths) = collect_output_paths(&entries);
        if let Err(err) = write_cache_raw_paths(&shown_dir_paths, &shown_file_paths) {
            eprintln!("failed to write --cache-raw files: {}", err);
        }
    }

    let detail_columns = build_detail_columns(&ctx);
    let is_list_mode = piped_output || cli.list || cli.header || !detail_columns.is_empty();

    let output = if is_list_mode {
        print_detailed_list(&entries, &ctx, &detail_columns)
    } else {
        let mut out = String::new();
        for entry in &entries {
            if entry.is_symlink && entry.broken_symlink {
                let mut broken_text =
                    get_display_name_text(&entry.render_name, &entry.metadata, &ctx);
                if ctx.show_targets {
                    if let Some(target) = entry.symlink_target.as_ref() {
                        broken_text.push_str(" -> ");
                        broken_text.push_str(&target.to_string_lossy());
                    }
                }
                out.push_str(&highlight_broken_symlink_text(
                    &broken_text,
                    ctx.color_enabled,
                ));
            } else {
                out.push_str(&get_styled_name(
                    &entry.render_name,
                    &entry.actual_path,
                    &entry.metadata,
                    &ctx,
                ));
                if ctx.show_targets {
                    if let Some(target) = entry.symlink_target.as_ref() {
                        out.push_str(" -> ");
                        out.push_str(&get_symlink_target_display(
                            &entry.actual_path,
                            target,
                            &ctx,
                        ));
                    }
                }
            }
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

fn populate_git_columns(listing_path: &Path, entries: &mut [EntryInfo]) -> (bool, bool) {
    let listing_abs = fs::canonicalize(listing_path).unwrap_or_else(|_| to_full_path(listing_path));

    let show_git_status = git_repo_root(&listing_abs).is_some();
    if show_git_status {
        let status_map = collect_git_statuses_for_listing(&listing_abs).unwrap_or_default();
        for entry in entries.iter_mut() {
            let status = status_map
                .get(&entry.display_name)
                .copied()
                .unwrap_or(('-', '-'));
            entry.git_status = Some(status);
        }
    }

    let mut show_repo_status = false;
    for entry in entries.iter_mut() {
        let is_dirish = if entry.is_symlink {
            entry.is_target_dir
        } else {
            entry.is_dir
        };
        if !is_dirish {
            continue;
        }
        let repo_status = git_repo_root_status(&entry.actual_path);
        if repo_status.is_some() {
            show_repo_status = true;
        }
        entry.repo_status = repo_status;
    }

    (show_git_status, show_repo_status)
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

fn paint_if_enabled(style: nu_ansi_term::Style, text: &str, enabled: bool) -> String {
    if enabled {
        style.paint(text).to_string()
    } else {
        text.to_string()
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

fn collect_recursive_stats(
    base_path: &Path,
    show_hidden: bool,
    dedupe_hardlinks: bool,
    need_sizes: bool,
    need_counts: bool,
) -> (HashMap<OsString, u64>, HashMap<OsString, (u64, u64)>) {
    if !need_sizes && !need_counts {
        return (HashMap::new(), HashMap::new());
    }

    let canonical_base = fs::canonicalize(base_path).unwrap_or_else(|_| base_path.to_path_buf());
    let scan_root = canonical_base.clone();
    // Top-level keyed aggregation: key is immediate child directory name.
    let dir_local_stats = Arc::new(Mutex::new(HashMap::<OsString, (u64, u64, u64)>::new()));
    let shared_stats = Arc::clone(&dir_local_stats);
    let seen_inodes = if need_sizes && dedupe_hardlinks {
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

            let rel = match current_path.strip_prefix(&canonical_base) {
                Ok(r) => r,
                Err(_) => return,
            };
            let mut rel_components = rel.components();
            let first_component = rel_components.next();

            let mut local_updates: HashMap<OsString, (u64, u64, u64)> = HashMap::new();

            // Root callback: seed top-level dir entries and add each top-level dir's own size.
            if first_component.is_none() {
                for child in children.iter_mut().filter_map(|e| e.as_mut().ok()) {
                    if !child.file_type().is_dir() {
                        continue;
                    }
                    let key = child.file_name().to_os_string();
                    let stats = local_updates.entry(key).or_insert((0, 0, 0));
                    if need_sizes {
                        if let Ok(metadata) = child.metadata() {
                            stats.0 += on_disk_size(&metadata);
                        }
                    }
                }
            } else {
                // Non-root callback: all children belong to the same top-level bucket.
                let top_level_name = match first_component {
                    Some(Component::Normal(name)) => name.to_os_string(),
                    _ => return,
                };

                let mut local_size = 0u64;
                let mut local_dirs = 0u64;
                let mut local_files = 0u64;
                let mut hardlink_candidates: Vec<(u64, u64, u64)> = Vec::new();

                for child in children.iter_mut().filter_map(|e| e.as_mut().ok()) {
                    let ft = child.file_type();
                    if need_counts {
                        if ft.is_dir() {
                            local_dirs += 1;
                        } else {
                            local_files += 1;
                        }
                    }
                    if need_sizes {
                        if let Ok(metadata) = child.metadata() {
                            if shared_seen.is_none() || metadata.is_dir() || metadata.nlink() <= 1 {
                                local_size += on_disk_size(&metadata);
                            } else {
                                hardlink_candidates.push((
                                    metadata.dev(),
                                    metadata.ino(),
                                    on_disk_size(&metadata),
                                ));
                            }
                        }
                    }
                }

                if let Some(ref seen) = shared_seen {
                    if !hardlink_candidates.is_empty() {
                        if let Ok(mut set) = seen.lock() {
                            for (dev, ino, size) in hardlink_candidates {
                                if set.insert((dev, ino)) {
                                    local_size += size;
                                }
                            }
                        }
                    }
                }

                local_updates.insert(top_level_name, (local_size, local_dirs, local_files));
            }

            if let Ok(mut stats) = shared_stats.lock() {
                for (key, value) in local_updates {
                    let entry = stats.entry(key).or_insert((0, 0, 0));
                    entry.0 += value.0;
                    entry.1 += value.1;
                    entry.2 += value.2;
                }
            }
        })
        .into_iter()
        .for_each(|_| {});

    let top_level_stats = match Arc::try_unwrap(dir_local_stats) {
        Ok(mutex) => mutex.into_inner().unwrap_or_default(),
        Err(shared) => shared.lock().map(|m| m.clone()).unwrap_or_default(),
    };

    let mut recursive_sizes: HashMap<OsString, u64> = HashMap::new();
    let mut recursive_counts: HashMap<OsString, (u64, u64)> = HashMap::new();

    for (name, stats) in top_level_stats {
        if need_sizes {
            recursive_sizes.insert(name.clone(), stats.0);
        }
        if need_counts {
            recursive_counts.insert(name, (stats.1, stats.2));
        }
    }

    (recursive_sizes, recursive_counts)
}

fn recursive_dir_on_disk_size(base_path: &Path, show_hidden: bool, dedupe_hardlinks: bool) -> u64 {
    let canonical_base = fs::canonicalize(base_path).unwrap_or_else(|_| base_path.to_path_buf());
    let mut total = 0u64;
    let mut seen_inodes = if dedupe_hardlinks {
        Some(HashSet::<(u64, u64)>::new())
    } else {
        None
    };

    for entry in WalkDir::new(&canonical_base).skip_hidden(!show_hidden) {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        let metadata = match entry.metadata() {
            Ok(m) => m,
            Err(_) => continue,
        };

        if let Some(seen) = seen_inodes.as_mut() {
            if !metadata.is_dir() && metadata.nlink() > 1 {
                let inode_key = (metadata.dev(), metadata.ino());
                if !seen.insert(inode_key) {
                    continue;
                }
            }
        }

        total += on_disk_size(&metadata);
    }

    total
}

fn create_entry_info(
    display_name: &str,
    actual_path: PathBuf,
    metadata: fs::Metadata,
    ctx: &Context,
    recursive_sizes: &HashMap<OsString, u64>,
    recursive_counts: &HashMap<OsString, (u64, u64)>,
    user_cache: &mut HashMap<u32, String>,
    group_cache: &mut HashMap<u32, String>,
) -> EntryInfo {
    let is_symlink = metadata.file_type().is_symlink();
    let is_dir = metadata.is_dir();
    let is_hidden = display_name.starts_with('.') && display_name != "." && display_name != "..";
    let render_name = if ctx.absolute {
        normalize_path_lexical(&to_full_path(&actual_path))
            .to_string_lossy()
            .into_owned()
    } else {
        display_name.to_string()
    };

    let mut is_target_dir = false;
    let mut symlink_target = None;
    let mut broken_symlink = false;
    let mut target_meta: Option<fs::Metadata> = None;
    if is_symlink {
        symlink_target = fs::read_link(&actual_path).ok();
        target_meta = fs::metadata(&actual_path).ok();
        is_target_dir = target_meta.as_ref().map(|m| m.is_dir()).unwrap_or(false);
        broken_symlink = target_meta.is_none();
    }

    let logical_size = if is_symlink && ctx.dereference && !broken_symlink {
        match target_meta.as_ref() {
            Some(meta) if meta.is_dir() => on_disk_size(meta),
            Some(meta) => meta.len(),
            None => {
                if is_dir {
                    on_disk_size(&metadata)
                } else {
                    metadata.len()
                }
            }
        }
    } else if is_dir {
        on_disk_size(&metadata)
    } else {
        metadata.len()
    };

    let true_size = if is_symlink && ctx.dereference && !broken_symlink {
        match target_meta.as_ref() {
            Some(meta) if meta.is_dir() => {
                recursive_dir_on_disk_size(&actual_path, ctx.show_hidden, ctx.dedupe_hardlinks)
            }
            Some(meta) => on_disk_size(meta),
            None => on_disk_size(&metadata),
        }
    } else if is_dir {
        recursive_sizes
            .get(OsStr::new(display_name))
            .copied()
            .unwrap_or_else(|| on_disk_size(&metadata))
    } else {
        on_disk_size(&metadata)
    };

    let final_size = if ctx.show_size_true {
        true_size
    } else {
        logical_size
    };
    let logical_size_str = format_size(logical_size);
    let true_size_str = format_size(true_size);
    let (dir_count, file_count) = if is_dir {
        recursive_counts
            .get(OsStr::new(display_name))
            .copied()
            .unwrap_or((0, 0))
    } else {
        (0, 0)
    };
    let dir_count_str = dir_count.to_string();
    let file_count_str = file_count.to_string();

    let user_str = if ctx.show_owner {
        user_cache
            .entry(metadata.uid())
            .or_insert_with(|| {
                get_user_by_uid(metadata.uid())
                    .map(|u| u.name().to_string_lossy().into_owned())
                    .unwrap_or_else(|| metadata.uid().to_string())
            })
            .clone()
    } else {
        String::new()
    };
    let group_str = if ctx.show_group {
        group_cache
            .entry(metadata.gid())
            .or_insert_with(|| {
                get_group_by_gid(metadata.gid())
                    .map(|g| g.name().to_string_lossy().into_owned())
                    .unwrap_or_else(|| metadata.gid().to_string())
            })
            .clone()
    } else {
        String::new()
    };

    let sort_mtime = if is_symlink && ctx.dereference && !broken_symlink {
        target_meta
            .as_ref()
            .map(|m| m.mtime())
            .unwrap_or_else(|| metadata.mtime())
    } else {
        metadata.mtime()
    };

    let time_str = if ctx.show_time {
        let mtime = sort_mtime;
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
        render_name,
        actual_path,
        metadata,
        is_symlink,
        is_dir,
        is_target_dir,
        is_hidden,
        logical_size_str,
        true_size_str,
        dir_count_str,
        file_count_str,
        user_str,
        group_str,
        time_str,
        final_size,
        dir_count,
        file_count,
        sort_mtime,
        symlink_target,
        broken_symlink,
        git_status: None,
        repo_status: None,
    }
}

fn print_detailed_list(entries: &[EntryInfo], ctx: &Context, columns: &[DetailColumn]) -> String {
    let (
        mut max_size_logical,
        mut max_size_true,
        mut max_dir_count,
        mut max_file_count,
        mut max_user,
        mut max_group,
        mut max_time,
    ) = (0, 0, 0, 0, 0, 0, 0);
    for e in entries {
        max_size_logical = max_size_logical.max(e.logical_size_str.len());
        max_size_true = max_size_true.max(e.true_size_str.len());
        max_dir_count = max_dir_count.max(e.dir_count_str.len());
        max_file_count = max_file_count.max(e.file_count_str.len());
        max_user = max_user.max(e.user_str.len());
        max_group = max_group.max(e.group_str.len());
        max_time = max_time.max(e.time_str.len());
    }
    if ctx.header {
        max_size_logical = max_size_logical.max("SIZE".len());
        max_size_true = max_size_true.max("TSIZE".len());
        max_dir_count = max_dir_count.max("DIRS".len());
        max_file_count = max_file_count.max("FILES".len());
        max_user = max_user.max("OWNER".len());
        max_group = max_group.max("GROUP".len());
        max_time = max_time.max("MODIFIED".len());
    }

    let mut out = String::new();
    let header_row = if ctx.header {
        let mut header = String::new();
        for column in columns {
            if !header.is_empty() {
                header.push(' ');
            }
            match column {
                DetailColumn::Perms => {
                    header.push_str(&format!("{:<10}", "PERMS"));
                }
                DetailColumn::SizeLogical => {
                    header.push_str(&format!("{:>width$}", "SIZE", width = max_size_logical));
                }
                DetailColumn::SizeTrue => {
                    header.push_str(&format!("{:>width$}", "TSIZE", width = max_size_true));
                }
                DetailColumn::DirCount => {
                    header.push_str(&format!("{:>width$}", "DIRS", width = max_dir_count));
                }
                DetailColumn::FileCount => {
                    header.push_str(&format!("{:>width$}", "FILES", width = max_file_count));
                }
                DetailColumn::Owner => {
                    header.push_str(&format!("{:<width$}", "OWNER", width = max_user));
                }
                DetailColumn::Time => {
                    header.push_str(&format!("{:<width$}", "MODIFIED", width = max_time));
                }
                DetailColumn::Group => {
                    header.push_str(&format!("{:<width$}", "GROUP", width = max_group));
                }
                DetailColumn::Git => {
                    header.push_str("GIT");
                }
            }
        }
        if !header.is_empty() {
            header.push(' ');
        }
        header.push_str("NAME");
        paint_if_enabled(nu_ansi_term::Style::default().bold(), &header, ctx.color_enabled)
    } else {
        String::new()
    };

    if ctx.header && !ctx.reverse {
        out.push_str(&header_row);
        out.push('\n');
    }

    for e in entries {
        let mut row = String::new();
        for column in columns {
            if !row.is_empty() {
                row.push(' ');
            }
            match column {
                DetailColumn::Perms => {
                    let ft = if e.is_dir {
                        paint_text_with_lscolors("d", &e.actual_path, &e.metadata, ctx)
                    } else if e.is_symlink {
                        paint_if_enabled(
                            nu_ansi_term::Color::LightCyan.bold(),
                            "l",
                            ctx.color_enabled,
                        )
                    } else {
                        paint_if_enabled(nu_ansi_term::Color::White.bold(), "-", ctx.color_enabled)
                    };
                    row.push_str(&ft);
                    row.push_str(&format_permissions(
                        e.metadata.permissions().mode(),
                        ctx.color_enabled,
                    ));
                }
                DetailColumn::SizeLogical => {
                    let size_text =
                        format!("{:>width$}", e.logical_size_str, width = max_size_logical);
                    row.push_str(&paint_if_enabled(
                        nu_ansi_term::Color::LightCyan.bold(),
                        &size_text,
                        ctx.color_enabled,
                    ));
                }
                DetailColumn::SizeTrue => {
                    let size_text = format!("{:>width$}", e.true_size_str, width = max_size_true);
                    row.push_str(&paint_if_enabled(
                        nu_ansi_term::Color::LightCyan.bold(),
                        &size_text,
                        ctx.color_enabled,
                    ));
                }
                DetailColumn::DirCount => {
                    let count_text = format!("{:>width$}", e.dir_count_str, width = max_dir_count);
                    row.push_str(&paint_if_enabled(
                        nu_ansi_term::Color::Yellow.bold(),
                        &count_text,
                        ctx.color_enabled,
                    ));
                }
                DetailColumn::FileCount => {
                    let count_text =
                        format!("{:>width$}", e.file_count_str, width = max_file_count);
                    row.push_str(&paint_if_enabled(
                        nu_ansi_term::Color::Yellow.bold(),
                        &count_text,
                        ctx.color_enabled,
                    ));
                }
                DetailColumn::Owner => {
                    row.push_str(&format!("{:<width$}", e.user_str, width = max_user));
                }
                DetailColumn::Time => {
                    let time_text = format!("{:<width$}", e.time_str, width = max_time);
                    row.push_str(&paint_if_enabled(
                        nu_ansi_term::Style::default().dimmed(),
                        &time_text,
                        ctx.color_enabled,
                    ));
                }
                DetailColumn::Group => {
                    row.push_str(&format!("{:<width$}", e.group_str, width = max_group));
                }
                DetailColumn::Git => {
                    if ctx.show_git {
                        let (staged, unstaged) = e.git_status.unwrap_or(('-', '-'));
                        row.push_str(&paint_if_enabled(
                            git_symbol_style(staged),
                            &staged.to_string(),
                            ctx.color_enabled,
                        ));
                        row.push_str(&paint_if_enabled(
                            git_symbol_style(unstaged),
                            &unstaged.to_string(),
                            ctx.color_enabled,
                        ));
                    }
                    if ctx.show_git_repos {
                        if ctx.show_git {
                            row.push(' ');
                        }
                        let repo_status = e.repo_status.unwrap_or(' ');
                        row.push_str(&paint_if_enabled(
                            git_repo_status_style(repo_status),
                            &repo_status.to_string(),
                            ctx.color_enabled,
                        ));
                    }
                }
            }
        }
        if !row.is_empty() {
            row.push(' ');
        }
        if e.is_symlink && e.broken_symlink {
            let mut broken_text = get_display_name_text(&e.render_name, &e.metadata, ctx);
            if ctx.show_targets {
                if let Some(target) = e.symlink_target.as_ref() {
                    broken_text.push_str(" -> ");
                    broken_text.push_str(&target.to_string_lossy());
                }
            }
            row.push_str(&highlight_broken_symlink_text(
                &broken_text,
                ctx.color_enabled,
            ));
        } else {
            row.push_str(&get_styled_name(
                &e.render_name,
                &e.actual_path,
                &e.metadata,
                ctx,
            ));
            if ctx.show_targets {
                if let Some(target) = e.symlink_target.as_ref() {
                    row.push_str(" -> ");
                    row.push_str(&get_symlink_target_display(&e.actual_path, target, ctx));
                }
            }
        }
        out.push_str(&row);
        out.push('\n');
    }

    if ctx.header && ctx.reverse {
        out.push_str(&header_row);
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
        return highlight_broken_symlink_text(&name, ctx.color_enabled);
    }
    if ctx.absolute {
        let abs_path = normalize_path_lexical(&to_full_path(actual_path));
        let abs_text = abs_path.to_string_lossy().into_owned();
        let suffix = name.strip_prefix(abs_text.as_str()).unwrap_or("");
        let (prefix, basename_core) = match abs_text.rfind('/') {
            Some(idx) if idx + 1 < abs_text.len() => (&abs_text[..idx + 1], &abs_text[idx + 1..]),
            _ => ("", abs_text.as_str()),
        };
        let basename = format!("{}{}", basename_core, suffix);
        let styled_basename = paint_text_with_lscolors(&basename, actual_path, metadata, ctx);
        let styled_prefix = if prefix.is_empty() {
            String::new()
        } else if !ctx.color_enabled {
            prefix.to_string()
        } else {
            format!("\x1b[38;2;255;255;255m{}\x1b[0m", prefix)
        };

        if ctx.hyperlink {
            let mut out = String::new();
            if !styled_prefix.is_empty() {
                let prefix_target = abs_path
                    .parent()
                    .map(|p| p.to_path_buf())
                    .unwrap_or_else(|| PathBuf::from("/"));
                out.push_str(&hyperlink_path(&prefix_target, &styled_prefix));
            }
            out.push_str(&hyperlink_path(&abs_path, &styled_basename));
            return out;
        }

        return format!("{}{}", styled_prefix, styled_basename);
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
    if !ctx.color_enabled {
        return text.to_string();
    }
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

fn get_classify_suffix(metadata: &fs::Metadata) -> Option<char> {
    let file_type = metadata.file_type();
    if file_type.is_symlink() {
        Some('@')
    } else if file_type.is_dir() {
        Some('/')
    } else if file_type.is_fifo() {
        Some('|')
    } else if file_type.is_socket() {
        Some('=')
    } else if metadata.permissions().mode() & 0o111 != 0 {
        Some('*')
    } else {
        None
    }
}

fn get_display_name_text(display_name: &str, metadata: &fs::Metadata, ctx: &Context) -> String {
    let mut name = display_name.to_string();
    if ctx.classify {
        if let Some(suffix) = get_classify_suffix(metadata) {
            name.push(suffix);
        }
    }
    name
}

fn highlight_broken_symlink_text(text: &str, color_enabled: bool) -> String {
    if color_enabled {
        format!("\x1b[48;2;255;0;0m\x1b[38;2;255;255;255m{}\x1b[0m", text)
    } else {
        text.to_string()
    }
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

fn normalize_path_lexical(path: &Path) -> PathBuf {
    let is_absolute = path.is_absolute();
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !out.pop() && !is_absolute {
                    out.push("..");
                }
            }
            Component::RootDir | Component::Prefix(_) | Component::Normal(_) => {
                out.push(component.as_os_str())
            }
        }
    }
    out
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
        let target_metadata = fs::symlink_metadata(path).ok();
        if ctx.classify {
            if let Some(m) = target_metadata.as_ref() {
                if let Some(suffix) = get_classify_suffix(m) {
                    if !display_text.ends_with(suffix) {
                        display_text.push(suffix);
                    }
                }
            }
        }

        let (split_text, trailing_suffix) = if display_text.len() > 1 && ctx.classify {
            let last_char = display_text.chars().last().unwrap();
            if ['/', '*', '@', '|', '='].contains(&last_char) {
                (
                    &display_text[..display_text.len() - 1],
                    &display_text[display_text.len() - 1..],
                )
            } else {
                (display_text.as_str(), "")
            }
        } else {
            (display_text.as_str(), "")
        };

        let (prefix, basename) = match split_text.rfind('/') {
            Some(idx) if idx + 1 < split_text.len() => {
                (&split_text[..idx + 1], &split_text[idx + 1..])
            }
            _ => ("", split_text),
        };
        let basename_with_suffix = format!("{}{}", basename, trailing_suffix);
        let target_style = ctx
            .lscolors
            .style_for_path_with_metadata(path, target_metadata.as_ref())
            .or_else(|| ctx.lscolors.style_for_path(path));
        let styled_basename = match target_style {
            Some(style) => {
                let ansi_style = Style::to_nu_ansi_term_style(style);
                if !ctx.color_enabled || ansi_style == nu_ansi_term::Style::default() {
                    basename_with_suffix
                } else {
                    ansi_style.paint(basename_with_suffix).to_string()
                }
            }
            None => basename_with_suffix,
        };
        if prefix.is_empty() {
            styled_basename
        } else if !ctx.color_enabled {
            format!("{}{}", prefix, styled_basename)
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

fn format_permissions(mode: u32, color_enabled: bool) -> String {
    if !color_enabled {
        let mut plain = String::new();
        for mask in [
            0o400, 0o200, 0o100, 0o040, 0o020, 0o010, 0o004, 0o002, 0o001,
        ] {
            let ch = match mask {
                0o400 | 0o040 | 0o004 => 'r',
                0o200 | 0o020 | 0o002 => 'w',
                _ => 'x',
            };
            plain.push(if mode & mask != 0 { ch } else { '-' });
        }
        return plain;
    }

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
