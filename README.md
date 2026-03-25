# twig

A fast `eza`-style directory lister focused on high-throughput top-level listing, strong symlink handling, Git-aware columns, and predictable size semantics.

## What This Project Is

`twig` is a single-binary Rust CLI that lists one directory level (`max_depth = 1`) with optional long-format metadata, sorting, Git integration, symlink target rendering, hyperlink support, and path caching for shell tooling.

Primary goals:
- Fast listing in large directories
- Good defaults (`--sort type`)
- Fine-grained output control
- Better symlink and Git visibility in long mode

## Project Structure

```text
.
├── .cargo/
│   └── config.toml      # local cargo build flags (target-cpu=native)
├── Cargo.toml          # crate metadata and dependencies
├── Cargo.lock          # locked dependency graph
├── src/
│   └── main.rs         # all CLI parsing, scanning, metadata collection, rendering
└── target/             # build artifacts (ignored in git)
```

### File Responsibilities

- `src/main.rs`
  - Defines CLI flags/options (`clap` derive)
  - Scans directory entries with `jwalk`
  - Computes size fields (`-s` vs `-S`)
  - Computes Git columns (`--git`, `--git-repos`)
  - Styles output using `LS_COLORS`
  - Handles symlink arrows/targets and broken-link highlighting
  - Writes optional raw path caches for shell integration
- `.cargo/config.toml`
  - Enables `-C target-cpu=native` for local optimized builds

## Build, Run, Install

### Build (release)

```bash
cargo build --release
```

Binary:

```text
./target/release/twig
```

### Run

```bash
./target/release/twig [OPTIONS] [PATH]
```

Default path is `.`.

## CLI Summary

From `twig --help`:

- `-a, --all` list all files, including hidden
- `-A, --almost-all` list hidden but exclude `.` and `..`
- `-l, --long` shorthand for `-psot`
- `-p, --permissions` show permission bits
- `-s, --size` show logical file size and allocated dir size
- `-o, --owner` show file owner
- `-g, --group` show group
- `-t, --modified` show mtime
- `-F, --classify` append classifier (`/`, `@`, `*`, etc.)
- `--sort <name|type|date|size>` sort key (default `type`)
- `-r, --reverse` reverse listing order
- `--hyperlink` render names as OSC8 hyperlinks
- `--git` show staged/unstaged two-character Git status
- `--git-repos` show repo-root status column
- `-S, --true-size` show allocated file size + recursive allocated dir size
- `-H, --no-dedupe-hardlinks` disable hardlink dedupe for `-S`
- `--cache-raw` write listed full paths for dirs/files to `/tmp/fzf-history-$USER/...`

## Operation Pipeline (Execution Order)

For each invocation, `twig` runs roughly this pipeline:

1. Parse CLI flags and build rendering context.
2. Optionally precompute recursive true-size totals (only for `-S`).
3. Scan one directory level using `jwalk` (`max_depth(1)`).
4. Build per-entry metadata struct:
   - file type
   - symlink target/broken state
   - size string + numeric sort size
   - owner/group/time strings
5. If `--hyperlink` is set and shown file count exceeds `1000`, hyperlinks are silently disabled.
6. Sort entries by selected key, apply reverse if requested.
7. Optionally populate Git columns:
   - `--git` status pair per top-level entry
   - `--git-repos` repo-root cleanliness marker
8. Optionally write raw path cache files (`--cache-raw`).
9. Render all rows into one buffered `String`.
10. Single `stdout.lock().write_all(...)` write.

## Size Semantics

`twig` intentionally separates logical size and on-disk size:

- `-s` / `--size`
  - Files: logical byte size (`metadata.len()`)
  - Dirs: allocated blocks for that directory entry (`st_blocks * 512`)

- `-S` / `--true-size`
  - Files: allocated blocks (`st_blocks * 512`)
  - Dirs: recursive allocated total (directory + descendants)

### Hardlink Deduplication (`-S` mode)

By default, `-S` deduplicates hardlinks by `(dev, ino)` while aggregating descendants.

- Default: dedupe on
- `-H` / `--no-dedupe-hardlinks`: dedupe off

## Git Integration

### `--git` two-character file status

Column format is:
- left: staged/index state
- right: unstaged/worktree state

Status symbols:
- `-` unmodified / no change in that side
- `M` modified
- `A` added to index
- `N` new untracked (worktree)
- `D` deleted
- `R` renamed
- `T` type-change
- `I` ignored
- `U` unmerged/conflicted

Common combinations:
- `MM` staged + unstaged modifications
- `M-` staged modified only
- `-M` unstaged modified only
- `A-` staged new file
- `AM` staged new file plus unstaged edits
- `D-` staged deletion
- `R-` staged rename
- `UU` conflict

Color mapping:
- `N`/`A`: green
- `M`/`R`/`T`: yellow
- `D`/`U`: red
- `-`/`I`: dimmed

### `--git-repos` repository status (directory roots)

Shown only for directory entries that are Git roots:
- `|` clean repo (green)
- `+` dirty repo (red)
- `~` unknown status (yellow)

## Symlink Behavior

- Classifier for symlink entries uses `@`.
- Long mode prints `name@ -> target`.
- Target rendering:
  - Prefix path segment before basename is forced white (`255,255,255`)
  - Basename uses `LS_COLORS`
  - If `-F` and target is a directory, target suffix `/` is shown
- Broken symlink rows are highlighted with red background and white foreground.
- Hyperlink mode can hyperlink link name and target path independently.

## LS_COLORS Integration

- Entry names are styled from `LS_COLORS`.
- Directory type marker `d` in permissions now uses the same directory style as name rendering.
- Symlink targets also resolve color via `LS_COLORS` where metadata is available.

## Hyperlink Guardrail

`--hyperlink` is automatically disabled (silently) when output would include more than `1000` non-directory entries. This avoids high ANSI/OSC8 overhead on large hot paths.

## Raw Cache Output (`--cache-raw`)

When enabled, `twig` writes full absolute paths of displayed entries to:

- Dirs:
  - `/tmp/fzf-history-$USER/universal-last-dirs-<fish_pid>`
- Files:
  - `/tmp/fzf-history-$USER/universal-last-files-<fish_pid>`

PID suffix resolution order:
1. `fish_pid` environment variable
2. parsed parent PID from `/proc/self/stat`
3. current process PID fallback

## Output Modes

- Compact mode (default): names separated by two spaces, single row
- Detailed mode: one row per entry when any of:
  - `-p`, `-s`, `-o`, `-g`, `-t`, `-l`, `--git`, `--git-repos`, `-S`

Detailed row columns are assembled left-to-right as enabled:
1. permissions
2. size
3. owner
4. group
5. modified time
6. git status pair (`--git`)
7. repo status marker (`--git-repos`)
8. styled name (and symlink target arrow when applicable)

## Practical Examples

```bash
# Fast type-sorted top-level list
./target/release/twig

# Long view equivalent to -psot
./target/release/twig -l ~/Dev

# Include hidden files and classify entries
./target/release/twig -aF ~

# True size with hardlink dedupe (default)
./target/release/twig -S ~/Downloads

# True size without hardlink dedupe
./target/release/twig -S -H ~/Downloads

# Git-aware long listing
./target/release/twig -l --git .

# Show Git root status for child directories
./target/release/twig -l --git-repos ~/src

# Reverse by date
./target/release/twig --sort date -r

# Emit shell cache files for last shown dirs/files
./target/release/twig --cache-raw ~/Dev
```

## Dependencies

Core crates:
- `clap` for CLI parsing
- `jwalk` for fast traversal
- `lscolors` + `nu-ansi-term` for styling
- `chrono` for timestamp formatting
- `users` for uid/gid resolution
- `jemallocator` for allocator performance

## Notes and Limits

- Current listing depth is one directory level.
- Git status is computed via `git status --porcelain=v1`.
- Repo-root marker is shown only for directories that are Git toplevel roots.
- Size units are decimal text with `K/M/G` suffixes, one decimal above bytes.

## Development

```bash
cargo fmt
cargo build --release
```

No separate test suite is included currently; behavior is validated via targeted fixture directories and manual command checks.
