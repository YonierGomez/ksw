# ksw — Developer Guide

Complete reference for building, releasing, and maintaining the project.

---

## Project Overview

| Item | Value |
|------|-------|
| Binary name | `ksw` |
| Repo | `git@github.com:YonierGomez/ksw.git` |
| Language | Go |
| Landing page | `index.html` (served via GitHub Pages on `main`) |
| Config file | `~/.ksw.json` |
| Homebrew tap | `git@github.com:YonierGomez/homebrew-ksw.git` |

---

## Repository Structure

```
.
├── main.go                  # All application code (single file)
├── ai.go                    # AI/Bedrock integration
├── go.mod / go.sum          # Go module files
├── install.sh               # One-line installer (auto-detects OS/arch)
├── index.html               # Landing page (GitHub Pages)
├── README.md                # User-facing documentation
├── CONTRIBUTING.md          # This file
├── Formula/
│   └── ksw.rb               # Homebrew formula (local copy — NOT the tap)
├── scripts/
│   └── release.sh           # Automated release script
├── demo/                    # VHS tapes and generated GIFs
└── .github/
    └── workflows/
        └── release.yml      # GitHub Actions: build + release on tag push
```

> **Note:** The actual Homebrew tap is a separate repo:
> `git@github.com:YonierGomez/homebrew-ksw.git`
> Local path: `/opt/homebrew/Library/Taps/yoniergomez/homebrew-ksw/`

---

## Version

The version is defined as a constant at the top of `main.go`:

```go
const version = "1.3.3"
```

All of the following must match on every release:

| Location | What to check |
|----------|--------------|
| `main.go` | `const version = "x.x.x"` |
| `Formula/ksw.rb` | `url` tag and `sha256` |
| Tap `Formula/ksw.rb` | Same as above |
| `index.html` | `softwareVersion`, badge `⎈ vX.Y.Z · AI-Powered`, footer |

---

## Build Locally

```bash
# Build for current platform
go build -o ksw .

# Test
./ksw -v
./ksw -h

# Cross-compile (same as CI)
GOOS=darwin  GOARCH=arm64  go build -ldflags "-s -w" -o dist/ksw-darwin-arm64  .
GOOS=darwin  GOARCH=amd64  go build -ldflags "-s -w" -o dist/ksw-darwin-amd64  .
GOOS=linux   GOARCH=amd64  go build -ldflags "-s -w" -o dist/ksw-linux-amd64   .
GOOS=linux   GOARCH=arm64  go build -ldflags "-s -w" -o dist/ksw-linux-arm64   .
```

---

## Branch Naming Convention

| Type | Pattern | Example |
|------|---------|---------|
| Feature | `feat/<description>` | `feat/copy-buttons` |
| Bug fix | `fix/<description>` | `fix/alias-double-at` |
| Chore | `chore/<description>` | `chore/sync-formula` |
| Docs | `docs/<description>` | `docs/linux-support` |

---

## Normal Change Flow

```bash
git checkout main && git pull origin main
git checkout -b <type>/<description>
# make changes
git add . && git commit -m "<type>: <description>"
git push origin <type>/<description>
gh pr create --repo YonierGomez/ksw --base main --head <branch> --title "<title>" --body "<description>"
gh pr merge --repo YonierGomez/ksw --squash <branch>
git checkout main && git pull origin main && git branch -D <branch>
```

> ⚠️ **main is protected** — all changes must go through a PR. Direct pushes are rejected.

---

## Release Process

The release is fully automated via `scripts/release.sh`.

### Prerequisites

- [`gh` CLI](https://cli.github.com/) installed and authenticated (`gh auth login`)
- `const version` in `main.go` already updated and merged to `main`
- Clean working tree on `main`

### Run the script

```bash
./scripts/release.sh <version> "<description>"
# Example:
./scripts/release.sh 1.3.4 "fix: corregir bug en alias"
```

### What the script does

1. Validates `gh` is authenticated
2. Checks `main` is clean and up to date
3. Verifies the tag doesn't already exist (local or remote)
4. Confirms `const version` in `main.go` matches the requested version
5. Checks all key files are present
6. Verifies the Homebrew tap directory exists locally
7. Creates and pushes the tag → triggers GitHub Actions
8. Waits for the tarball to be available on GitHub
9. Calculates `sha256` from the tarball
10. Verifies the tarball contains the correct `const version`
11. Updates `index.html` version automatically
12. Opens and merges a PR in `YonierGomez/ksw` with the updated formula
13. Opens and merges a PR in `YonierGomez/homebrew-ksw` with the updated formula
14. Runs `brew upgrade ksw` and verifies `ksw -v` matches the new version

### If `brew upgrade` says "already installed" but `ksw -v` shows wrong version

```bash
brew reinstall ksw
```

---

## GitHub Actions Workflow

File: `.github/workflows/release.yml`

**Trigger:** Push of a tag matching `v*`

**Steps:**
1. Checkout code
2. Set up Go (version from `go.mod`)
3. Build 4 binaries (darwin/linux × amd64/arm64)
4. Create tarballs + `checksums.txt`
5. Create GitHub Release with all artifacts

---

## Landing Page (index.html)

- Served via GitHub Pages from `main` branch
- URL: `https://yoniergomez.github.io/ksw/`
- Single HTML file with inline CSS and JS — no build step needed
- Version is updated automatically by `scripts/release.sh`

---

## install.sh

One-line installer that auto-detects OS and architecture:

```bash
curl -sL https://raw.githubusercontent.com/YonierGomez/ksw/main/install.sh | bash
```

The script:
1. Detects OS (`darwin` / `linux`) and arch (`amd64` / `arm64`)
2. Fetches latest version from GitHub API
3. Downloads the tarball from GitHub Releases
4. Extracts and installs to `/usr/local/bin/ksw`

---

## Dependencies

```
github.com/charmbracelet/bubbletea  — TUI framework
github.com/charmbracelet/lipgloss   — Terminal styling
```

Update with:

```bash
go get -u ./...
go mod tidy
```
