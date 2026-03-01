# ksw — Developer Guide

Complete reference for building, releasing, and maintaining the project.

---

## Project Overview

| Item | Value |
|------|-------|
| Binary name | `ksw` |
| Repo | `github.com/YonierGomez/ksw` |
| Language | Go |
| Landing page | `index.html` (served via GitHub Pages on `main`) |
| Config file | `~/.ksw.json` |
| Homebrew tap | `github.com/YonierGomez/homebrew-ksw` |

---

## Repository Structure

```
.
├── main.go                  # All application code (single file)
├── go.mod / go.sum          # Go module files
├── install.sh               # One-line installer (auto-detects OS/arch)
├── index.html               # Landing page (GitHub Pages)
├── README.md                # User-facing documentation
├── CONTRIBUTING.md          # This file
├── Formula/
│   └── ksw.rb               # Homebrew formula (local copy — NOT the tap)
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
const version = "1.1.3"
```

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

## Release Process

### 1. Make changes on a feature branch

```bash
git checkout main && git pull origin main
git checkout -b feat/my-feature   # or fix/... or bump/...
# ... make changes ...
git add <files>
git commit -m "feat: description"
git push origin feat/my-feature
```

> ⚠️ **main is protected** — all changes must go through a PR. Direct pushes are rejected.

### 2. Open a PR and merge

Open PR at: `https://github.com/YonierGomez/ksw/pull/new/<branch-name>`

### 3. Bump the version

After merging, bump `version` in `main.go`:

```go
const version = "1.1.4"  // increment patch/minor/major as needed
```

Create a PR for the bump:

```bash
git checkout main && git pull origin main
git checkout -b bump/v1.1.4
# edit main.go: const version = "1.1.4"
git add main.go
git commit -m "bump: v1.1.4 - short description"
git push origin bump/v1.1.4
# open PR, merge it
```

### 4. Create and push the tag

After the bump PR is merged:

```bash
git checkout main && git pull origin main
git tag v1.1.4
git push origin v1.1.4
```

> This triggers the GitHub Actions release workflow automatically.

### 5. Verify the release

Check the workflow at: `https://github.com/YonierGomez/ksw/actions`

The workflow builds 4 binaries + tarballs + checksums.txt and creates a GitHub Release.

---

## Update Homebrew Formula

After the release is published, get the new sha256:

```bash
curl -sL https://github.com/YonierGomez/ksw/archive/refs/tags/v1.1.4.tar.gz | shasum -a 256
```

Then update the tap formula:

```bash
cd /opt/homebrew/Library/Taps/yoniergomez/homebrew-ksw
# Edit Formula/ksw.rb:
#   url  → new tag URL
#   sha256 → new hash
git add Formula/ksw.rb
git commit -m "fix: update formula for v1.1.4"
git push origin HEAD
```

Also update the local copy in this repo:

```bash
cp /opt/homebrew/Library/Taps/yoniergomez/homebrew-ksw/Formula/ksw.rb Formula/ksw.rb
git add Formula/ksw.rb
git commit -m "chore: sync Formula/ksw.rb for v1.1.4"
# include in a PR
```

Test the update:

```bash
brew update && brew upgrade ksw
ksw -v  # should show new version
```

---

## Branch Naming Convention

| Type | Pattern | Example |
|------|---------|---------|
| Feature | `feat/<description>` | `feat/copy-buttons` |
| Bug fix | `fix/<description>` | `fix/short-mode-color` |
| Version bump | `bump/v<version>` | `bump/v1.1.4` |
| Docs | `docs/<description>` | `docs/linux-support` |
| Chore | `chore/<description>` | `chore/sync-formula` |

---

## Commit Message Convention

```
<type>: <short description>

Types: feat, fix, docs, chore, bump, ci, refactor
```

Examples:
```
feat: add copy buttons to install commands
fix: apply green color to context name in short mode header
bump: v1.1.3 - fix short mode header color, help improvements
docs: add macOS and Linux availability to README and landing page
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

**Release body** is defined inline in the workflow YAML. To update the install instructions shown in the release notes, edit `.github/workflows/release.yml`.

---

## Landing Page (index.html)

- Served via GitHub Pages from `main` branch
- URL: `https://yoniergomez.github.io/ksw/`
- Single HTML file with inline CSS and JS — no build step needed
- Version badge in hero section is hardcoded — update it when releasing

To update the version badge in `index.html`:

```bash
# Search for: v1.1.2 (or current version)
grep -n "v1.1" index.html
# Update the badge line and the structured data version
```

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

## Key Files to Update on Each Release

| File | What to update |
|------|---------------|
| `main.go` | `const version = "x.x.x"` |
| `index.html` | Version badge in hero (`v1.1.x`) and structured data |
| `Formula/ksw.rb` | `url` and `sha256` |
| Tap repo `Formula/ksw.rb` | Same as above |
| `.github/workflows/release.yml` | Release body (if install instructions change) |

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
