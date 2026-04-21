use chrono::{DateTime, Datelike, Local};
use clap::{Parser, ValueEnum};
use jemallocator::Jemalloc;
use jwalk::WalkDir;
use lscolors::{LsColors, Style};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::ffi::{OsStr, OsString};
use std::fs;
use std::io::{self, IsTerminal, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::FileTypeExt;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use users::{get_group_by_gid, get_user_by_uid};

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const AUTO_STYLE_MAX_ENTRIES: usize = 1000;
const NTFS_FS_TYPES: [&str; 3] = ["ntfs", "ntfs3", "fuseblk"];

#[derive(Clone)]
struct MountInfo {
    device: PathBuf,
    mount_point: PathBuf,
    fs_type: String,
}

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
enum OutputWhen {
    Always,
    Auto,
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

    /// List directories themselves, not their contents
    #[arg(short = 'd', long = "directory")]
    directory: bool,

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
    #[arg(long, value_enum, default_value = "auto")]
    color: OutputWhen,

    /// Render names as terminal hyperlinks (default: never; plain -U means auto)
    #[arg(
        short = 'U',
        long,
        value_enum,
        default_value = "never",
        default_missing_value = "auto",
        num_args = 0..=1,
        require_equals = true
    )]
    hyperlink: OutputWhen,

    /// Show symlink targets
    #[arg(short = 'x', long = "show-targets")]
    show_targets: bool,

    /// Show absolute paths in output
    #[arg(short = 'X', long = "absolute")]
    absolute: bool,

    /// Dereference symlink targets for size/time calculations
    #[arg(short = 'D', long = "dereference")]
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
    sort_counts_total: bool,
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

fn push_long_shorthand_columns(columns: &mut Vec<DetailColumn>) {
    push_unique_column(columns, DetailColumn::Perms);
    push_unique_column(columns, DetailColumn::SizeLogical);
    push_unique_column(columns, DetailColumn::Owner);
    push_unique_column(columns, DetailColumn::Time);
}

fn dot_entry_rank(name: &str) -> Option<u8> {
    match name {
        "." => Some(0),
        ".." => Some(1),
        _ => None,
    }
}

fn pin_dot_entries_top(entries: &mut Vec<EntryInfo>) {
    if entries.is_empty() {
        return;
    }

    let mut pinned: Vec<(u8, EntryInfo)> = Vec::new();
    let mut rest: Vec<EntryInfo> = Vec::with_capacity(entries.len());

    for entry in entries.drain(..) {
        if let Some(rank) = dot_entry_rank(&entry.display_name) {
            pinned.push((rank, entry));
        } else {
            rest.push(entry);
        }
    }

    pinned.sort_by_key(|(rank, _)| *rank);
    entries.extend(pinned.into_iter().map(|(_, entry)| entry));
    entries.extend(rest);
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
                "long" => push_long_shorthand_columns(&mut columns),
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
                    'l' => push_long_shorthand_columns(&mut columns),
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

fn sort_was_explicitly_set() -> bool {
    let mut stop_parsing_flags = false;
    let mut expect_sort_value = false;

    for arg in std::env::args_os().skip(1) {
        let arg = arg.to_string_lossy();

        if expect_sort_value {
            return true;
        }
        if stop_parsing_flags {
            continue;
        }
        if arg == "--" {
            stop_parsing_flags = true;
            continue;
        }
        if arg == "--sort" {
            expect_sort_value = true;
            continue;
        }
        if arg.starts_with("--sort=") {
            return true;
        }
    }

    false
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ImplicitSort {
    Counts,
    TrueSize,
}

fn implicit_sort_from_flag_order() -> Option<ImplicitSort> {
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
                "counts" => return Some(ImplicitSort::Counts),
                "true-size" => return Some(ImplicitSort::TrueSize),
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
                    'c' => return Some(ImplicitSort::Counts),
                    'S' => return Some(ImplicitSort::TrueSize),
                    _ => {}
                }
            }
        }
    }

    None
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

struct FastEntry {
    display_name: String,
    actual_path: PathBuf,
    is_hidden: bool,
    is_dir: bool,
    is_symlink: bool,
    is_target_dir: bool,
}

struct LongFastEntry {
    display_name: String,
    actual_path: PathBuf,
    metadata: fs::Metadata,
    is_hidden: bool,
    is_dir: bool,
    is_symlink: bool,
    is_target_dir: bool,
    logical_size: u64,
    logical_size_str: String,
    user_str: String,
    time_str: String,
    sort_mtime: i64,
    symlink_target: Option<PathBuf>,
    broken_symlink: bool,
}

fn can_use_large_dir_fast_path(cli: &Cli, input_is_dir: bool) -> bool {
    input_is_dir
        && !cli.directory
        && !cli.long
        && !cli.list
        && !cli.header
        && !cli.permissions
        && !cli.size
        && !cli.counts
        && !cli.owner
        && !cli.group
        && !cli.modified
        && !cli.classify
        && !cli.show_targets
        && !cli.absolute
        && !cli.dereference
        && !cli.git
        && !cli.true_size
        && matches!(cli.sort, SortBy::Name | SortBy::Type)
}

fn can_use_large_dir_long_fast_path(cli: &Cli, input_is_dir: bool) -> bool {
    input_is_dir
        && !cli.directory
        && cli.long
        && !cli.list
        && !cli.permissions
        && !cli.size
        && !cli.counts
        && !cli.owner
        && !cli.group
        && !cli.modified
        && !cli.classify
        && !cli.show_targets
        && !cli.absolute
        && !cli.dereference
        && !cli.git
        && !cli.true_size
        && !cli.header
}

fn fast_type_rank(e: &FastEntry) -> u8 {
    if e.is_symlink && e.is_target_dir {
        0
    } else if e.is_dir {
        1
    } else if e.is_symlink {
        2
    } else {
        3
    }
}

fn collect_fast_entries(base_path: &Path, show_hidden: bool) -> io::Result<Vec<FastEntry>> {
    let mut entries = Vec::new();
    for item in fs::read_dir(base_path)? {
        let Ok(dir_entry) = item else {
            continue;
        };
        let file_name = dir_entry.file_name().to_string_lossy().to_string();
        let is_hidden = file_name.starts_with('.');
        if is_hidden && !show_hidden {
            continue;
        }

        let actual_path = dir_entry.path();
        let ft = match dir_entry
            .file_type()
            .or_else(|_| fs::symlink_metadata(&actual_path).map(|m| m.file_type()))
        {
            Ok(v) => v,
            Err(_) => continue,
        };
        let is_symlink = ft.is_symlink();
        let is_dir = ft.is_dir();
        let is_target_dir = if is_symlink {
            fs::metadata(&actual_path)
                .map(|m| m.is_dir())
                .unwrap_or(false)
        } else {
            is_dir
        };
        entries.push(FastEntry {
            display_name: file_name,
            actual_path,
            is_hidden,
            is_dir,
            is_symlink,
            is_target_dir,
        });
    }
    Ok(entries)
}

fn pin_dot_entries_top_fast(entries: &mut Vec<FastEntry>) {
    if entries.is_empty() {
        return;
    }

    let mut pinned: Vec<(u8, FastEntry)> = Vec::new();
    let mut rest: Vec<FastEntry> = Vec::with_capacity(entries.len());
    for entry in entries.drain(..) {
        if let Some(rank) = dot_entry_rank(&entry.display_name) {
            pinned.push((rank, entry));
        } else {
            rest.push(entry);
        }
    }
    pinned.sort_by_key(|(rank, _)| *rank);
    entries.extend(pinned.into_iter().map(|(_, entry)| entry));
    entries.extend(rest);
}

fn paint_name_fast(name: &str, path: &Path, ctx: &Context) -> String {
    if !ctx.color_enabled {
        return name.to_string();
    }
    match ctx.lscolors.style_for_path(path) {
        Some(style) => {
            let ansi_style = Style::to_nu_ansi_term_style(style);
            if ansi_style == nu_ansi_term::Style::default() {
                name.to_string()
            } else {
                ansi_style.paint(name).to_string()
            }
        }
        None => name.to_string(),
    }
}

fn try_render_large_dir_fast_path(
    cli: &Cli,
    ctx: &mut Context,
    input_is_dir: bool,
    sort_explicit: bool,
    show_hidden: bool,
    piped_output: bool,
    cache_raw_enabled: bool,
) -> Option<String> {
    if !can_use_large_dir_fast_path(cli, input_is_dir) {
        return None;
    }

    let mut entries = collect_fast_entries(Path::new(&cli.path), show_hidden).ok()?;

    if cli.all {
        entries.push(FastEntry {
            display_name: ".".to_string(),
            actual_path: PathBuf::from(&cli.path),
            is_hidden: false,
            is_dir: true,
            is_symlink: false,
            is_target_dir: true,
        });
        let parent_path = if cli.path == "." {
            PathBuf::from("..")
        } else {
            PathBuf::from(format!("{}/..", cli.path))
        };
        entries.push(FastEntry {
            display_name: "..".to_string(),
            actual_path: parent_path,
            is_hidden: false,
            is_dir: true,
            is_symlink: false,
            is_target_dir: true,
        });
    }

    if entries.len() <= AUTO_STYLE_MAX_ENTRIES {
        return None;
    }

    let over_auto_limit = true;
    ctx.color_enabled = output_enabled(cli.color, piped_output, over_auto_limit);
    ctx.hyperlink = output_enabled(cli.hyperlink, piped_output, over_auto_limit);

    entries.sort_by(|a, b| {
        match cli.sort {
            SortBy::Type => {
                let a_rank = fast_type_rank(a);
                let b_rank = fast_type_rank(b);
                if a_rank != b_rank {
                    return a_rank.cmp(&b_rank);
                }
            }
            SortBy::Name => {}
            _ => {}
        }
        if a.is_hidden != b.is_hidden {
            return b.is_hidden.cmp(&a.is_hidden);
        }
        a.display_name.cmp(&b.display_name)
    });
    if cli.reverse {
        entries.reverse();
    }
    if cli.all && !sort_explicit {
        pin_dot_entries_top_fast(&mut entries);
    }

    if cache_raw_enabled {
        let mut dir_paths = Vec::new();
        let mut file_paths = Vec::new();
        for e in &entries {
            let full_path = normalize_path_lexical(&to_full_path(&e.actual_path));
            let is_dir = if e.is_symlink { e.is_target_dir } else { e.is_dir };
            if is_dir {
                dir_paths.push(full_path);
            } else {
                file_paths.push(full_path);
            }
        }
        if let Err(err) = write_cache_raw_paths(&dir_paths, &file_paths) {
            eprintln!("failed to write --cache-raw files: {}", err);
        }
    }

    let mut out = String::new();
    for (idx, entry) in entries.iter().enumerate() {
        if idx > 0 {
            out.push_str("  ");
        }
        let painted = paint_name_fast(&entry.display_name, &entry.actual_path, ctx);
        if ctx.hyperlink {
            out.push_str(&hyperlink_path(&entry.actual_path, &painted));
        } else {
            out.push_str(&painted);
        }
    }
    out.push('\n');
    Some(out)
}

fn long_fast_type_rank(e: &LongFastEntry) -> u8 {
    if e.is_symlink && e.is_target_dir {
        0
    } else if e.is_dir {
        1
    } else if e.is_symlink {
        2
    } else {
        3
    }
}

fn format_time_display(mtime: i64, now_year: i32, now_timestamp: i64) -> String {
    let dt: DateTime<Local> = DateTime::from_timestamp(mtime, 0)
        .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap())
        .with_timezone(&Local);
    if now_year == dt.year() && (now_timestamp - dt.timestamp()).abs() < 15552000 {
        dt.format("%e %b %H:%M").to_string()
    } else {
        dt.format("%e %b  %Y").to_string()
    }
}

fn make_long_fast_entry(
    display_name: String,
    actual_path: PathBuf,
    metadata: fs::Metadata,
    now_year: i32,
    now_timestamp: i64,
    user_cache: &mut HashMap<u32, String>,
) -> LongFastEntry {
    let is_symlink = metadata.file_type().is_symlink();
    let is_dir = metadata.is_dir();
    let is_hidden = display_name.starts_with('.') && display_name != "." && display_name != "..";
    let mut symlink_target = None;
    let mut is_target_dir = false;
    let mut broken_symlink = false;
    if is_symlink {
        symlink_target = fs::read_link(&actual_path).ok();
        let target_meta = fs::metadata(&actual_path).ok();
        is_target_dir = target_meta.as_ref().map(|m| m.is_dir()).unwrap_or(false);
        broken_symlink = target_meta.is_none();
    }
    let logical_size = if is_dir {
        on_disk_size(&metadata)
    } else {
        metadata.len()
    };
    let logical_size_str = format_size(logical_size);
    let uid = metadata.uid();
    let user_str = user_cache
        .entry(uid)
        .or_insert_with(|| {
            get_user_by_uid(uid)
                .map(|u| u.name().to_string_lossy().into_owned())
                .unwrap_or_else(|| uid.to_string())
        })
        .clone();
    let sort_mtime = metadata.mtime();
    let time_str = format_time_display(sort_mtime, now_year, now_timestamp);

    LongFastEntry {
        display_name,
        actual_path,
        metadata,
        is_hidden,
        is_dir,
        is_symlink,
        is_target_dir,
        logical_size,
        logical_size_str,
        user_str,
        time_str,
        sort_mtime,
        symlink_target,
        broken_symlink,
    }
}

fn collect_long_fast_entries(
    base_path: &Path,
    show_hidden: bool,
    now_year: i32,
    now_timestamp: i64,
    user_cache: &mut HashMap<u32, String>,
) -> io::Result<Vec<LongFastEntry>> {
    let mut entries = Vec::new();
    for item in fs::read_dir(base_path)? {
        let Ok(dir_entry) = item else {
            continue;
        };
        let file_name = dir_entry.file_name().to_string_lossy().to_string();
        let is_hidden = file_name.starts_with('.');
        if is_hidden && !show_hidden {
            continue;
        }
        let actual_path = dir_entry.path();
        let metadata = match fs::symlink_metadata(&actual_path) {
            Ok(m) => m,
            Err(_) => continue,
        };
        entries.push(make_long_fast_entry(
            file_name,
            actual_path,
            metadata,
            now_year,
            now_timestamp,
            user_cache,
        ));
    }
    Ok(entries)
}

fn pin_dot_entries_top_long_fast(entries: &mut Vec<LongFastEntry>) {
    if entries.is_empty() {
        return;
    }

    let mut pinned: Vec<(u8, LongFastEntry)> = Vec::new();
    let mut rest: Vec<LongFastEntry> = Vec::with_capacity(entries.len());
    for entry in entries.drain(..) {
        if let Some(rank) = dot_entry_rank(&entry.display_name) {
            pinned.push((rank, entry));
        } else {
            rest.push(entry);
        }
    }
    pinned.sort_by_key(|(rank, _)| *rank);
    entries.extend(pinned.into_iter().map(|(_, entry)| entry));
    entries.extend(rest);
}

fn try_render_large_dir_long_fast_path(
    cli: &Cli,
    ctx: &mut Context,
    input_is_dir: bool,
    sort_explicit: bool,
    show_hidden: bool,
    piped_output: bool,
    cache_raw_enabled: bool,
) -> Option<String> {
    if !can_use_large_dir_long_fast_path(cli, input_is_dir) {
        return None;
    }

    let now = Local::now();
    let now_year = now.year();
    let now_timestamp = now.timestamp();
    let mut user_cache = HashMap::<u32, String>::new();
    let mut entries = collect_long_fast_entries(
        Path::new(&cli.path),
        show_hidden,
        now_year,
        now_timestamp,
        &mut user_cache,
    )
    .ok()?;

    if cli.all {
        if let Ok(meta) = fs::symlink_metadata(&cli.path) {
            entries.push(make_long_fast_entry(
                ".".to_string(),
                PathBuf::from(&cli.path),
                meta,
                now_year,
                now_timestamp,
                &mut user_cache,
            ));
        }
        let parent_path = if cli.path == "." {
            "..".to_string()
        } else {
            format!("{}/..", cli.path)
        };
        if let Ok(meta) = fs::symlink_metadata(&parent_path) {
            entries.push(make_long_fast_entry(
                "..".to_string(),
                PathBuf::from(parent_path),
                meta,
                now_year,
                now_timestamp,
                &mut user_cache,
            ));
        }
    }

    if entries.len() <= AUTO_STYLE_MAX_ENTRIES {
        return None;
    }

    let over_auto_limit = true;
    ctx.color_enabled = output_enabled(cli.color, piped_output, over_auto_limit);
    ctx.hyperlink = output_enabled(cli.hyperlink, piped_output, over_auto_limit);

    entries.sort_by(|a, b| {
        match cli.sort {
            SortBy::Size => {
                if a.logical_size != b.logical_size {
                    return b.logical_size.cmp(&a.logical_size);
                }
                let a_name = a.display_name.trim_start_matches('.').to_lowercase();
                let b_name = b.display_name.trim_start_matches('.').to_lowercase();
                return a_name.cmp(&b_name);
            }
            SortBy::Date => {
                if a.sort_mtime != b.sort_mtime {
                    return b.sort_mtime.cmp(&a.sort_mtime);
                }
            }
            SortBy::Type => {
                let a_rank = long_fast_type_rank(a);
                let b_rank = long_fast_type_rank(b);
                if a_rank != b_rank {
                    return a_rank.cmp(&b_rank);
                }
            }
            SortBy::Name => {}
            _ => {}
        }
        if a.is_hidden != b.is_hidden {
            return b.is_hidden.cmp(&a.is_hidden);
        }
        a.display_name.cmp(&b.display_name)
    });
    if cli.reverse {
        entries.reverse();
    }
    if cli.all && !sort_explicit {
        pin_dot_entries_top_long_fast(&mut entries);
    }

    if cache_raw_enabled {
        let mut dir_paths = Vec::new();
        let mut file_paths = Vec::new();
        for e in &entries {
            let full_path = normalize_path_lexical(&to_full_path(&e.actual_path));
            let is_dir = if e.is_symlink { e.is_target_dir } else { e.is_dir };
            if is_dir {
                dir_paths.push(full_path);
            } else {
                file_paths.push(full_path);
            }
        }
        if let Err(err) = write_cache_raw_paths(&dir_paths, &file_paths) {
            eprintln!("failed to write --cache-raw files: {}", err);
        }
    }

    let mut max_size = 0usize;
    let mut max_user = 0usize;
    let mut max_time = 0usize;
    for e in &entries {
        max_size = max_size.max(e.logical_size_str.len());
        max_user = max_user.max(e.user_str.len());
        max_time = max_time.max(e.time_str.len());
    }

    let mut out = String::new();
    for e in &entries {
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
        out.push_str(&ft);
        out.push_str(&format_permissions(e.metadata.permissions().mode(), ctx.color_enabled));
        out.push(' ');

        let size_text = format!("{:>width$}", e.logical_size_str, width = max_size);
        out.push_str(&paint_if_enabled(
            nu_ansi_term::Color::LightCyan.bold(),
            &size_text,
            ctx.color_enabled,
        ));
        out.push(' ');

        out.push_str(&format!("{:<width$}", e.user_str, width = max_user));
        out.push(' ');

        let time_text = format!("{:<width$}", e.time_str, width = max_time);
        out.push_str(&paint_if_enabled(
            nu_ansi_term::Style::default().dimmed(),
            &time_text,
            ctx.color_enabled,
        ));
        out.push(' ');

        if e.is_symlink && e.broken_symlink {
            let mut broken_text = e.display_name.clone();
            if ctx.show_targets {
                if let Some(target) = e.symlink_target.as_ref() {
                    broken_text.push_str(" -> ");
                    broken_text.push_str(&target.to_string_lossy());
                }
            }
            out.push_str(&highlight_broken_symlink_text(&broken_text, ctx.color_enabled));
        } else {
            let painted_name = paint_text_with_lscolors(&e.display_name, &e.actual_path, &e.metadata, ctx);
            if ctx.hyperlink {
                out.push_str(&hyperlink_path(&e.actual_path, &painted_name));
            } else {
                out.push_str(&painted_name);
            }
            if ctx.show_targets {
                if let Some(target) = e.symlink_target.as_ref() {
                    out.push_str(" -> ");
                    out.push_str(&get_symlink_target_display(&e.actual_path, target, ctx));
                }
            }
        }

        out.push('\n');
    }

    Some(out)
}

fn output_enabled(mode: OutputWhen, piped_output: bool, over_auto_limit: bool) -> bool {
    match mode {
        OutputWhen::Always => true,
        OutputWhen::Auto => !piped_output && !over_auto_limit,
        OutputWhen::Never => false,
    }
}

fn main() {
    let cli = Cli::parse();
    let sort_explicit = sort_was_explicitly_set();
    let implicit_sort = if sort_explicit {
        None
    } else {
        implicit_sort_from_flag_order()
    };
    let effective_sort = match implicit_sort {
        Some(ImplicitSort::Counts) => SortBy::DirCount,
        Some(ImplicitSort::TrueSize) => SortBy::Size,
        None => cli.sort,
    };
    let implicit_ascending_sort = implicit_sort.is_some();
    let sort_counts_total = matches!(implicit_sort, Some(ImplicitSort::Counts));
    let show_hidden = cli.all || cli.almost_all;

    let piped_output = !io::stdout().is_terminal();
    let color_enabled = output_enabled(cli.color, piped_output, false);
    let classify_enabled = cli.classify && !piped_output;
    let hyperlink_enabled = output_enabled(cli.hyperlink, piped_output, false);
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
        sort_by: effective_sort,
        sort_counts_total,
    };
    let mut entries = Vec::new();
    let need_counts = cli.counts || matches!(effective_sort, SortBy::DirCount | SortBy::FileCount);
    let (recursive_sizes, recursive_counts, root_true_size) = collect_recursive_stats(
        Path::new(&cli.path),
        show_hidden,
        cli.dedupe_hardlinks,
        cli.true_size,
        need_counts,
    );
    let now = Local::now();
    let now_year = now.year();
    let now_timestamp = now.timestamp();
    let mut user_cache: HashMap<u32, String> = HashMap::new();
    let mut group_cache: HashMap<u32, String> = HashMap::new();

    let input_path = Path::new(&cli.path);
    let input_meta = fs::symlink_metadata(input_path).ok();
    let input_is_dir = input_meta.as_ref().map(|m| m.is_dir()).unwrap_or(false);

    if let Some(output) = try_render_large_dir_long_fast_path(
        &cli,
        &mut ctx,
        input_is_dir,
        sort_explicit,
        show_hidden,
        piped_output,
        cache_raw_enabled,
    ) {
        let mut stdout = io::stdout().lock();
        let _ = stdout.write_all(output.as_bytes());
        return;
    }

    if let Some(output) = try_render_large_dir_fast_path(
        &cli,
        &mut ctx,
        input_is_dir,
        sort_explicit,
        show_hidden,
        piped_output,
        cache_raw_enabled,
    ) {
        let mut stdout = io::stdout().lock();
        let _ = stdout.write_all(output.as_bytes());
        return;
    }

    if cli.all && input_is_dir && !cli.directory {
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
                now_year,
                now_timestamp,
            ));
        }
        if !cli.true_size {
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
                    now_year,
                    now_timestamp,
                ));
            }
        }
    }

    if input_is_dir && !cli.directory {
        let read_dir = match fs::read_dir(&cli.path) {
            Ok(v) => v,
            Err(_) => return,
        };

        for dir_entry in read_dir {
            let dir_entry = match dir_entry {
                Ok(e) => e,
                Err(_) => continue,
            };
            let file_name = dir_entry.file_name().to_string_lossy().to_string();
            let is_hidden = file_name.starts_with('.');
            if is_hidden && !show_hidden {
                continue;
            }
            let entry_path = dir_entry.path();
            let metadata = match fs::symlink_metadata(&entry_path) {
                Ok(m) => m,
                Err(_) => continue,
            };
            entries.push(create_entry_info(
                &file_name,
                entry_path,
                metadata,
                &ctx,
                &recursive_sizes,
                &recursive_counts,
                &mut user_cache,
                &mut group_cache,
                now_year,
                now_timestamp,
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
            now_year,
            now_timestamp,
        ));
    }

    if cli.all && cli.true_size && input_is_dir && !cli.directory {
        let dot_true_size = root_true_size.unwrap_or_else(|| {
            recursive_dir_on_disk_size(Path::new(&cli.path), show_hidden, cli.dedupe_hardlinks)
        });
        if let Some(dot_entry) = entries.iter_mut().find(|e| e.display_name == ".") {
            dot_entry.true_size_str = format_size(dot_true_size);
            dot_entry.final_size = dot_true_size;
        }
    }
    if cli.directory && cli.true_size && input_is_dir {
        if let Some(dir_entry) = entries.get_mut(0) {
            if let Some(total_true_size) = root_true_size {
                dir_entry.true_size_str = format_size(total_true_size);
                dir_entry.final_size = total_true_size;
            }
        }
    }

    if entries.is_empty() {
        if cache_raw_enabled {
            let _ = write_cache_raw_paths(&[], &[]);
        }
        return;
    }

    let over_auto_limit = entries.len() > AUTO_STYLE_MAX_ENTRIES;
    ctx.color_enabled = output_enabled(cli.color, piped_output, over_auto_limit);
    ctx.hyperlink = output_enabled(cli.hyperlink, piped_output, over_auto_limit);

    let reverse_sorted_output = cli.reverse ^ implicit_ascending_sort;
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
                let a_count = if ctx.sort_counts_total {
                    a.dir_count.saturating_add(a.file_count)
                } else {
                    a.dir_count
                };
                let b_count = if ctx.sort_counts_total {
                    b.dir_count.saturating_add(b.file_count)
                } else {
                    b.dir_count
                };
                if a_count != b_count {
                    return b_count.cmp(&a_count);
                }
                if ctx.sort_counts_total && a.dir_count != b.dir_count {
                    // For implicit -c total sorting, ties prefer files over dirs
                    // in ascending output after the final reverse pass.
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
    if reverse_sorted_output {
        entries.reverse();
    }
    if cli.all && input_is_dir && !sort_explicit {
        pin_dot_entries_top(&mut entries);
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
        for (idx, entry) in entries.iter().enumerate() {
            if idx > 0 {
                out.push_str("  ");
            }
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
        let full_path = normalize_path_lexical(&to_full_path(&entry.actual_path));
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

fn is_hidden_name(name: &OsStr) -> bool {
    name.as_bytes().first().copied() == Some(b'.')
}

fn unescape_proc_mount_field(field: &str) -> String {
    let bytes = field.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 3 < bytes.len() {
            let a = bytes[i + 1];
            let b = bytes[i + 2];
            let c = bytes[i + 3];
            let octal = (b'0'..=b'7').contains(&a)
                && (b'0'..=b'7').contains(&b)
                && (b'0'..=b'7').contains(&c);
            if octal {
                let value = ((a - b'0') << 6) | ((b - b'0') << 3) | (c - b'0');
                out.push(value);
                i += 4;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn detect_mount_info(path: &Path) -> Option<MountInfo> {
    let canonical = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let mounts = fs::read_to_string("/proc/mounts").ok()?;
    let mut best: Option<(usize, MountInfo)> = None;

    for line in mounts.lines() {
        let mut parts = line.split_whitespace();
        let (device_raw, mount_point_raw, fs_type) =
            match (parts.next(), parts.next(), parts.next()) {
                (Some(device), Some(mount), Some(fs_type)) => (device, mount, fs_type),
                _ => continue,
            };
        let device = PathBuf::from(unescape_proc_mount_field(device_raw));
        let mount_point = PathBuf::from(unescape_proc_mount_field(mount_point_raw));
        if !canonical.starts_with(&mount_point) {
            continue;
        }
        let mount_len = mount_point.as_os_str().as_bytes().len();
        if best
            .as_ref()
            .map(|(best_len, _)| mount_len > *best_len)
            .unwrap_or(true)
        {
            best = Some((
                mount_len,
                MountInfo {
                    device,
                    mount_point,
                    fs_type: fs_type.to_string(),
                },
            ));
        }
    }

    best.map(|(_, info)| info)
}

fn detect_filesystem_type(path: &Path) -> Option<String> {
    detect_mount_info(path).map(|info| info.fs_type)
}

fn is_ntfs_like_filesystem(path: &Path) -> bool {
    detect_filesystem_type(path)
        .map(|fs_type| NTFS_FS_TYPES.iter().any(|t| fs_type == *t))
        .unwrap_or(false)
}

fn ntfs_best_filename(
    entry: &ntfs::NtfsIndexEntry<'_, ntfs::indexes::NtfsFileNameIndex>,
) -> Option<String> {
    if let Some(Ok(file_name)) = entry.key() {
        let name = file_name.name().to_string_lossy().to_string();
        if !name.contains('~') || name.len() > 12 {
            return Some(name);
        }
    }
    entry
        .key()
        .and_then(|result| result.ok())
        .map(|file_name| file_name.name().to_string_lossy().to_string())
}

fn ntfs_is_reparse_point(file: &ntfs::NtfsFile, device: &mut fs::File) -> bool {
    let mut attrs = file.attributes();
    while let Some(attr_result) = attrs.next(device) {
        if let Ok(attr_item) = attr_result {
            if let Ok(attr) = attr_item.to_attribute() {
                if let Ok(attr_ty) = attr.ty() {
                    if attr_ty == ntfs::NtfsAttributeType::ReparsePoint {
                        return true;
                    }
                }
            }
        }
    }
    false
}

fn ntfs_file_logical_size(file: &ntfs::NtfsFile, device: &mut fs::File) -> u64 {
    if let Some(data_attr) = file.data(device, "") {
        if let Ok(data_item) = data_attr {
            if let Ok(data_attr_obj) = data_item.to_attribute() {
                if let Ok(value) = data_attr_obj.value(device) {
                    return value.len();
                }
            }
        }
    }
    0
}

fn fs_block_size(path: &Path) -> u64 {
    let path_c = match std::ffi::CString::new(path.as_os_str().as_bytes()) {
        Ok(c) => c,
        Err(_) => return 4096,
    };
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(path_c.as_ptr(), &mut stat as *mut _) };
    if rc == 0 && stat.f_frsize > 0 {
        stat.f_frsize as u64
    } else {
        4096
    }
}

fn round_up_to_block(size: u64, block_size: u64) -> u64 {
    if size == 0 {
        return 0;
    }
    if block_size <= 1 {
        return size;
    }
    size.div_ceil(block_size) * block_size
}

fn ntfs_find_subdir_record(
    ntfs: &ntfs::Ntfs,
    device: &mut fs::File,
    start_record: u64,
    rel_path: &Path,
) -> Option<u64> {
    let mut current_record = start_record;
    if rel_path.as_os_str().is_empty() {
        return Some(current_record);
    }

    for component in rel_path.components() {
        let name = match component {
            Component::Normal(name) => name.to_string_lossy().to_string(),
            _ => continue,
        };
        let dir_file = ntfs.file(device, current_record).ok()?;
        let index = dir_file.directory_index(device).ok()?;
        let mut entries = index.entries();
        let mut seen_records = HashSet::<u64>::new();
        let mut next_record: Option<u64> = None;

        while let Some(entry_result) = entries.next(device) {
            let entry = match entry_result {
                Ok(e) => e,
                Err(_) => continue,
            };
            let entry_name = match ntfs_best_filename(&entry) {
                Some(n) => n,
                None => continue,
            };
            if entry_name == "." || entry_name == ".." {
                continue;
            }
            let child_record = entry.file_reference().file_record_number();
            if !seen_records.insert(child_record) {
                continue;
            }
            if entry_name == name {
                next_record = Some(child_record);
                break;
            }
        }
        current_record = next_record?;
    }

    Some(current_record)
}

fn ntfs_scan_subtree_record(
    ntfs: &ntfs::Ntfs,
    device: &mut fs::File,
    top_record: u64,
    show_hidden: bool,
    need_sizes: bool,
    need_counts: bool,
    block_size: u64,
    shared_seen: Option<&Arc<Mutex<HashSet<u64>>>>,
) -> (u64, u64, u64) {
    let mut total_size = 0u64;
    let mut total_dirs = 0u64;
    let mut total_files = 0u64;
    let mut stack = vec![top_record];
    let mut seen_dirs = HashSet::<u64>::new();

    while let Some(current_record) = stack.pop() {
        if !seen_dirs.insert(current_record) {
            continue;
        }
        let dir_file = match ntfs.file(device, current_record) {
            Ok(file) => file,
            Err(_) => continue,
        };
        if need_sizes {
            total_size += block_size;
        }
        let index = match dir_file.directory_index(device) {
            Ok(i) => i,
            Err(_) => continue,
        };
        let mut entries = index.entries();
        let mut seen_records = HashSet::<u64>::new();

        while let Some(entry_result) = entries.next(device) {
            let entry = match entry_result {
                Ok(e) => e,
                Err(_) => continue,
            };
            let name = match ntfs_best_filename(&entry) {
                Some(n) => n,
                None => continue,
            };
            if name == "." || name == ".." {
                continue;
            }
            if !show_hidden && name.starts_with('.') {
                continue;
            }
            let child_record = entry.file_reference().file_record_number();
            if !seen_records.insert(child_record) {
                continue;
            }
            let child_file = match ntfs.file(device, child_record) {
                Ok(f) => f,
                Err(_) => continue,
            };
            let child_is_dir = child_file.is_directory();
            let child_is_reparse = ntfs_is_reparse_point(&child_file, device);

            if child_is_dir && !child_is_reparse {
                if need_counts {
                    total_dirs += 1;
                }
                stack.push(child_record);
            } else {
                if need_counts {
                    total_files += 1;
                }
                if need_sizes {
                    let mut include_size = true;
                    if let Some(seen) = shared_seen {
                        if let Ok(mut set) = seen.lock() {
                            include_size = set.insert(child_record);
                        }
                    }
                    if include_size {
                        let logical_size = ntfs_file_logical_size(&child_file, device);
                        total_size += round_up_to_block(logical_size, block_size);
                    }
                }
            }
        }
    }

    (total_size, total_dirs, total_files)
}

fn collect_recursive_stats_ntfs_mft(
    base_path: &Path,
    show_hidden: bool,
    dedupe_hardlinks: bool,
    need_sizes: bool,
    need_counts: bool,
) -> io::Result<(
    HashMap<OsString, u64>,
    HashMap<OsString, (u64, u64)>,
    Option<u64>,
)> {
    let canonical_base = fs::canonicalize(base_path).unwrap_or_else(|_| base_path.to_path_buf());
    let mount = detect_mount_info(&canonical_base)
        .ok_or_else(|| io::Error::other("mount detection failed"))?;
    if !NTFS_FS_TYPES.iter().any(|t| mount.fs_type == *t) {
        return Err(io::Error::other("not ntfs"));
    }

    let mut device = fs::File::open(&mount.device)?;
    let ntfs = ntfs::Ntfs::new(&mut device).map_err(|err| io::Error::other(err.to_string()))?;
    let root_dir = ntfs
        .root_directory(&mut device)
        .map_err(|err| io::Error::other(err.to_string()))?;
    let root_record = root_dir.file_record_number();
    let rel_path = canonical_base
        .strip_prefix(&mount.mount_point)
        .unwrap_or(Path::new(""));
    let base_record = ntfs_find_subdir_record(&ntfs, &mut device, root_record, rel_path)
        .ok_or_else(|| io::Error::other("base directory not found in mft"))?;

    let block_size = fs_block_size(&canonical_base);
    let available_threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let ntfs_threads = std::env::var("TWIG_NTFS_THREADS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or_else(|| available_threads.min(4).max(1));
    let mut recursive_sizes: HashMap<OsString, u64> = HashMap::new();
    let mut recursive_counts: HashMap<OsString, (u64, u64)> = HashMap::new();
    let mut top_level_dirs: Vec<(OsString, u64)> = Vec::new();
    let shared_seen = if need_sizes && dedupe_hardlinks {
        Some(Arc::new(Mutex::new(HashSet::<u64>::new())))
    } else {
        None
    };
    let mut root_recursive_size = if need_sizes {
        fs::symlink_metadata(&canonical_base)
            .map(|m| on_disk_size(&m))
            .unwrap_or(block_size)
    } else {
        0
    };

    let base_file = ntfs
        .file(&mut device, base_record)
        .map_err(|err| io::Error::other(err.to_string()))?;
    let index = base_file
        .directory_index(&mut device)
        .map_err(|err| io::Error::other(err.to_string()))?;
    let mut entries = index.entries();
    let mut seen_records = HashSet::<u64>::new();

    while let Some(entry_result) = entries.next(&mut device) {
        let entry = match entry_result {
            Ok(e) => e,
            Err(_) => continue,
        };
        let name = match ntfs_best_filename(&entry) {
            Some(n) => n,
            None => continue,
        };
        if name == "." || name == ".." {
            continue;
        }
        if !show_hidden && name.starts_with('.') {
            continue;
        }
        let child_record = entry.file_reference().file_record_number();
        if !seen_records.insert(child_record) {
            continue;
        }
        let child_file = match ntfs.file(&mut device, child_record) {
            Ok(file) => file,
            Err(_) => continue,
        };
        let child_is_dir = child_file.is_directory();
        let child_is_reparse = ntfs_is_reparse_point(&child_file, &mut device);

        if child_is_dir && !child_is_reparse {
            top_level_dirs.push((OsString::from(name), child_record));
        } else if need_sizes {
            let mut include_size = true;
            if let Some(seen) = shared_seen.as_ref() {
                if let Ok(mut set) = seen.lock() {
                    include_size = set.insert(child_record);
                }
            }
            if include_size {
                root_recursive_size +=
                    round_up_to_block(ntfs_file_logical_size(&child_file, &mut device), block_size);
            }
        }
    }

    let run_scan = |dirs: Vec<(OsString, u64)>| {
        dirs.into_par_iter()
            .map(|(name, record)| {
                let mut thread_device = fs::File::open(&mount.device).map_err(|_| ())?;
                let thread_ntfs = ntfs::Ntfs::new(&mut thread_device).map_err(|_| ())?;
                let (size, dirs_count, file_count) = ntfs_scan_subtree_record(
                    &thread_ntfs,
                    &mut thread_device,
                    record,
                    show_hidden,
                    need_sizes,
                    need_counts,
                    block_size,
                    shared_seen.as_ref(),
                );
                Ok::<(OsString, u64, u64, u64), ()>((name, size, dirs_count, file_count))
            })
            .filter_map(Result::ok)
            .collect::<Vec<_>>()
    };

    let dir_results: Vec<(OsString, u64, u64, u64)> = if ntfs_threads <= 1 || top_level_dirs.len() <= 1
    {
        top_level_dirs
            .into_iter()
            .filter_map(|(name, record)| {
                let (size, dirs_count, file_count) = ntfs_scan_subtree_record(
                    &ntfs,
                    &mut device,
                    record,
                    show_hidden,
                    need_sizes,
                    need_counts,
                    block_size,
                    shared_seen.as_ref(),
                );
                Some((name, size, dirs_count, file_count))
            })
            .collect()
    } else if let Ok(pool) = rayon::ThreadPoolBuilder::new()
        .num_threads(ntfs_threads)
        .build()
    {
        pool.install(|| run_scan(top_level_dirs))
    } else {
        run_scan(top_level_dirs)
    };

    for (name, size, dirs_count, file_count) in dir_results {
        if need_sizes {
            recursive_sizes.insert(name.clone(), size);
            root_recursive_size += size;
        }
        if need_counts {
            recursive_counts.insert(name, (dirs_count, file_count));
        }
    }

    Ok((
        recursive_sizes,
        recursive_counts,
        if need_sizes {
            Some(root_recursive_size)
        } else {
            None
        },
    ))
}

fn size_with_hardlink_dedupe(
    metadata: &fs::Metadata,
    shared_seen: Option<&Arc<Mutex<HashSet<(u64, u64)>>>>,
) -> u64 {
    let size = on_disk_size(metadata);
    if metadata.is_dir() || metadata.nlink() <= 1 || shared_seen.is_none() {
        return size;
    }
    if let Some(seen) = shared_seen {
        if let Ok(mut set) = seen.lock() {
            if set.insert((metadata.dev(), metadata.ino())) {
                return size;
            }
            return 0;
        }
    }
    size
}

fn scan_subtree_stats_low_overhead(
    top_dir: &Path,
    show_hidden: bool,
    need_sizes: bool,
    need_counts: bool,
    shared_seen: Option<&Arc<Mutex<HashSet<(u64, u64)>>>>,
) -> (u64, u64, u64) {
    let mut total_size = 0u64;
    let mut total_dirs = 0u64;
    let mut total_files = 0u64;

    if need_sizes {
        if let Ok(meta) = fs::symlink_metadata(top_dir) {
            total_size += size_with_hardlink_dedupe(&meta, shared_seen);
        }
    }

    let mut stack = vec![top_dir.to_path_buf()];
    while let Some(current_dir) = stack.pop() {
        let entries = match fs::read_dir(&current_dir) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for entry_res in entries {
            let entry = match entry_res {
                Ok(e) => e,
                Err(_) => continue,
            };
            let name = entry.file_name();
            if !show_hidden && is_hidden_name(&name) {
                continue;
            }
            let path = entry.path();
            let metadata = match fs::symlink_metadata(&path) {
                Ok(m) => m,
                Err(_) => continue,
            };
            let is_dir = metadata.file_type().is_dir();

            if need_counts {
                if is_dir {
                    total_dirs += 1;
                } else {
                    total_files += 1;
                }
            }

            if need_sizes {
                total_size += size_with_hardlink_dedupe(&metadata, shared_seen);
            }

            if is_dir {
                stack.push(path);
            }
        }
    }

    (total_size, total_dirs, total_files)
}

fn collect_recursive_stats_ntfs(
    base_path: &Path,
    show_hidden: bool,
    dedupe_hardlinks: bool,
    need_sizes: bool,
    need_counts: bool,
) -> (
    HashMap<OsString, u64>,
    HashMap<OsString, (u64, u64)>,
    Option<u64>,
) {
    if !need_sizes && !need_counts {
        return (HashMap::new(), HashMap::new(), None);
    }

    let ntfs_debug = std::env::var_os("TWIG_NTFS_DEBUG").is_some();
    match collect_recursive_stats_ntfs_mft(
        base_path,
        show_hidden,
        dedupe_hardlinks,
        need_sizes,
        need_counts,
    ) {
        Ok(stats) => {
            if ntfs_debug {
                eprintln!("twig: NTFS MFT fast path enabled");
            }
            return stats;
        }
        Err(err) => {
            if ntfs_debug {
                eprintln!("twig: NTFS MFT fast path unavailable: {}", err);
            }
        }
    }

    let canonical_base = fs::canonicalize(base_path).unwrap_or_else(|_| base_path.to_path_buf());
    let mut recursive_sizes: HashMap<OsString, u64> = HashMap::new();
    let mut recursive_counts: HashMap<OsString, (u64, u64)> = HashMap::new();
    let mut top_level_dirs: Vec<(OsString, PathBuf)> = Vec::new();
    let available_threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let ntfs_threads = std::env::var("TWIG_NTFS_THREADS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or_else(|| available_threads.min(4).max(1));
    let shared_seen = if need_sizes && dedupe_hardlinks {
        Some(Arc::new(Mutex::new(HashSet::<(u64, u64)>::new())))
    } else {
        None
    };
    let root_size_total = if need_sizes {
        fs::symlink_metadata(&canonical_base)
            .map(|m| size_with_hardlink_dedupe(&m, shared_seen.as_ref()))
            .unwrap_or(0)
    } else {
        0
    };
    let mut root_recursive_size = root_size_total;

    let entries = match fs::read_dir(&canonical_base) {
        Ok(e) => e,
        Err(_) => {
            return (
                recursive_sizes,
                recursive_counts,
                if need_sizes {
                    Some(root_recursive_size)
                } else {
                    None
                },
            );
        }
    };

    for entry_res in entries {
        let entry = match entry_res {
            Ok(e) => e,
            Err(_) => continue,
        };
        let name = entry.file_name();
        if !show_hidden && is_hidden_name(&name) {
            continue;
        }
        let child_path = entry.path();
        if need_sizes {
            let metadata = match fs::symlink_metadata(&child_path) {
                Ok(m) => m,
                Err(_) => continue,
            };
            if metadata.file_type().is_dir() {
                top_level_dirs.push((name, child_path));
            } else {
                root_recursive_size += size_with_hardlink_dedupe(&metadata, shared_seen.as_ref());
            }
            continue;
        }
        let file_type = match entry.file_type() {
            Ok(ft) => ft,
            Err(_) => continue,
        };
        if file_type.is_dir() {
            top_level_dirs.push((name, child_path));
        }
    }

    let run_scan = |dirs: Vec<(OsString, PathBuf)>| {
        dirs.into_par_iter()
            .map(|(name, dir_path)| {
                let (size, dirs, files) = scan_subtree_stats_low_overhead(
                    &dir_path,
                    show_hidden,
                    need_sizes,
                    need_counts,
                    shared_seen.as_ref(),
                );
                (name, size, dirs, files)
            })
            .collect::<Vec<_>>()
    };

    let dir_results: Vec<(OsString, u64, u64, u64)> = if ntfs_threads <= 1 || top_level_dirs.len() <= 1
    {
        top_level_dirs
            .into_iter()
            .map(|(name, dir_path)| {
                let (size, dirs, files) = scan_subtree_stats_low_overhead(
                    &dir_path,
                    show_hidden,
                    need_sizes,
                    need_counts,
                    shared_seen.as_ref(),
                );
                (name, size, dirs, files)
            })
            .collect()
    } else if let Ok(pool) = rayon::ThreadPoolBuilder::new()
        .num_threads(ntfs_threads)
        .build()
    {
        pool.install(|| run_scan(top_level_dirs))
    } else {
        run_scan(top_level_dirs)
    };

    for (name, size, dirs, files) in dir_results {
        if need_sizes {
            recursive_sizes.insert(name.clone(), size);
            root_recursive_size += size;
        }
        if need_counts {
            recursive_counts.insert(name, (dirs, files));
        }
    }

    (
        recursive_sizes,
        recursive_counts,
        if need_sizes {
            Some(root_recursive_size)
        } else {
            None
        },
    )
}

fn collect_recursive_stats(
    base_path: &Path,
    show_hidden: bool,
    dedupe_hardlinks: bool,
    need_sizes: bool,
    need_counts: bool,
) -> (
    HashMap<OsString, u64>,
    HashMap<OsString, (u64, u64)>,
    Option<u64>,
) {
    if !need_sizes && !need_counts {
        return (HashMap::new(), HashMap::new(), None);
    }

    let canonical_base = fs::canonicalize(base_path).unwrap_or_else(|_| base_path.to_path_buf());
    if is_ntfs_like_filesystem(&canonical_base) {
        return collect_recursive_stats_ntfs(
            &canonical_base,
            show_hidden,
            dedupe_hardlinks,
            need_sizes,
            need_counts,
        );
    }
    let scan_root = canonical_base.clone();
    // Top-level keyed aggregation: key is immediate child directory name.
    let dir_local_stats = Arc::new(Mutex::new(HashMap::<OsString, (u64, u64, u64)>::new()));
    let shared_stats = Arc::clone(&dir_local_stats);
    let root_size_total = if need_sizes {
        let initial = fs::symlink_metadata(&canonical_base)
            .map(|m| on_disk_size(&m))
            .unwrap_or(0);
        Some(Arc::new(Mutex::new(initial)))
    } else {
        None
    };
    let shared_root_size = root_size_total.as_ref().map(Arc::clone);
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
            let mut callback_size = 0u64;

            // Root callback: seed top-level dir entries and add each top-level dir's own size.
            if first_component.is_none() {
                let mut hardlink_candidates: Vec<(u64, u64, u64)> = Vec::new();
                for child in children.iter_mut().filter_map(|e| e.as_mut().ok()) {
                    let ft = child.file_type();
                    if !need_sizes {
                        continue;
                    }
                    if let Ok(metadata) = child.metadata() {
                        let size = on_disk_size(&metadata);
                        if ft.is_dir() {
                            callback_size += size;
                            let key = child.file_name().to_os_string();
                            let stats = local_updates.entry(key).or_insert((0, 0, 0));
                            stats.0 += size;
                        } else if shared_seen.is_none() || metadata.nlink() <= 1 {
                            callback_size += size;
                        } else {
                            hardlink_candidates.push((metadata.dev(), metadata.ino(), size));
                        }
                    }
                }
                if let Some(ref seen) = shared_seen {
                    if !hardlink_candidates.is_empty() {
                        if let Ok(mut set) = seen.lock() {
                            for (dev, ino, size) in hardlink_candidates {
                                if set.insert((dev, ino)) {
                                    callback_size += size;
                                }
                            }
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

                callback_size = local_size;
                local_updates.insert(top_level_name, (local_size, local_dirs, local_files));
            }

            if let Some(ref root_size) = shared_root_size {
                if callback_size > 0 {
                    if let Ok(mut total) = root_size.lock() {
                        *total += callback_size;
                    }
                }
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

    let root_recursive_size = if need_sizes {
        root_size_total.map(|shared| match Arc::try_unwrap(shared) {
            Ok(mutex) => mutex.into_inner().unwrap_or(0),
            Err(shared_again) => shared_again.lock().map(|v| *v).unwrap_or(0),
        })
    } else {
        None
    };

    (recursive_sizes, recursive_counts, root_recursive_size)
}

fn recursive_dir_on_disk_size(base_path: &Path, show_hidden: bool, dedupe_hardlinks: bool) -> u64 {
    let canonical_base = fs::canonicalize(base_path).unwrap_or_else(|_| base_path.to_path_buf());
    if is_ntfs_like_filesystem(&canonical_base) {
        let shared_seen = if dedupe_hardlinks {
            Some(Arc::new(Mutex::new(HashSet::<(u64, u64)>::new())))
        } else {
            None
        };
        let (size, _, _) = scan_subtree_stats_low_overhead(
            &canonical_base,
            show_hidden,
            true,
            false,
            shared_seen.as_ref(),
        );
        return size;
    }
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
    now_year: i32,
    now_timestamp: i64,
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
        (0, 1)
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
        format_time_display(sort_mtime, now_year, now_timestamp)
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

    if let Some(path) = resolved_target.as_deref() {
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
                let prefix_target = path
                    .parent()
                    .map(|p| p.to_path_buf())
                    .unwrap_or_else(|| PathBuf::from("/"));
                out.push_str(&hyperlink_path(&prefix_target, &styled_prefix));
            }
            out.push_str(&hyperlink_path(path, &styled_basename));
            return out;
        }

        if styled_prefix.is_empty() {
            styled_basename
        } else {
            format!("{}{}", styled_prefix, styled_basename)
        }
    } else {
        display_text
    }
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
