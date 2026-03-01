#!/bin/sh
# ksw installer — https://github.com/YonierGomez/ksw
# Usage: curl -sL https://raw.githubusercontent.com/YonierGomez/ksw/main/install.sh | bash

set -e

REPO="YonierGomez/ksw"
BINARY="ksw"
INSTALL_DIR="/usr/local/bin"

# ── Colors ──────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
GRAY='\033[0;37m'
BOLD='\033[1m'
RESET='\033[0m'

info()    { printf "${CYAN}→${RESET} %s\n" "$1"; }
success() { printf "${GREEN}✔${RESET} ${BOLD}%s${RESET}\n" "$1"; }
error()   { printf "${RED}✗${RESET} %s\n" "$1" >&2; exit 1; }

# ── Detect OS ───────────────────────────────────────────
detect_os() {
  case "$(uname -s)" in
    Darwin) echo "darwin" ;;
    Linux)  echo "linux"  ;;
    *)      error "Unsupported OS: $(uname -s). Only macOS and Linux are supported." ;;
  esac
}

# ── Detect Architecture ─────────────────────────────────
detect_arch() {
  case "$(uname -m)" in
    x86_64|amd64)   echo "amd64" ;;
    arm64|aarch64)  echo "arm64" ;;
    *)              error "Unsupported architecture: $(uname -m)." ;;
  esac
}

# ── Get latest version ──────────────────────────────────
get_latest_version() {
  if command -v curl >/dev/null 2>&1; then
    curl -sL "https://api.github.com/repos/${REPO}/releases/latest" \
      | grep '"tag_name"' \
      | sed 's/.*"tag_name": *"\(.*\)".*/\1/'
  elif command -v wget >/dev/null 2>&1; then
    wget -qO- "https://api.github.com/repos/${REPO}/releases/latest" \
      | grep '"tag_name"' \
      | sed 's/.*"tag_name": *"\(.*\)".*/\1/'
  else
    error "curl or wget is required to install ksw."
  fi
}

# ── Download ────────────────────────────────────────────
download() {
  url="$1"
  dest="$2"
  if command -v curl >/dev/null 2>&1; then
    curl -sL "$url" -o "$dest"
  elif command -v wget >/dev/null 2>&1; then
    wget -qO "$dest" "$url"
  fi
}

# ── Main ────────────────────────────────────────────────
main() {
  printf "\n${BOLD}  ⎈ ksw installer${RESET}\n\n"

  OS=$(detect_os)
  ARCH=$(detect_arch)

  info "Detected: ${OS}/${ARCH}"

  VERSION=$(get_latest_version)
  if [ -z "$VERSION" ]; then
    error "Could not determine latest version. Check your internet connection."
  fi

  info "Latest version: ${VERSION}"

  TARBALL="ksw-${OS}-${ARCH}.tar.gz"
  URL="https://github.com/${REPO}/releases/download/${VERSION}/${TARBALL}"

  # Download to temp dir
  TMP_DIR=$(mktemp -d)
  trap 'rm -rf "$TMP_DIR"' EXIT

  info "Downloading ${TARBALL}..."
  download "$URL" "${TMP_DIR}/${TARBALL}"

  # Extract
  tar -xzf "${TMP_DIR}/${TARBALL}" -C "$TMP_DIR"

  EXTRACTED_BINARY="${TMP_DIR}/ksw-${OS}-${ARCH}"
  if [ ! -f "$EXTRACTED_BINARY" ]; then
    error "Binary not found after extraction. Expected: ${EXTRACTED_BINARY}"
  fi

  chmod +x "$EXTRACTED_BINARY"

  # Install
  if [ -w "$INSTALL_DIR" ]; then
    mv "$EXTRACTED_BINARY" "${INSTALL_DIR}/${BINARY}"
  else
    info "Installing to ${INSTALL_DIR} (requires sudo)..."
    sudo mv "$EXTRACTED_BINARY" "${INSTALL_DIR}/${BINARY}"
  fi

  # Verify
  if command -v ksw >/dev/null 2>&1; then
    INSTALLED_VERSION=$(ksw -v 2>/dev/null || echo "unknown")
    success "Installed ${INSTALLED_VERSION} → ${INSTALL_DIR}/${BINARY}"
    printf "\n${GRAY}  Run: ${CYAN}ksw${RESET}\n\n"
  else
    success "Installed → ${INSTALL_DIR}/${BINARY}"
    printf "\n${GRAY}  Make sure ${INSTALL_DIR} is in your PATH, then run: ${CYAN}ksw${RESET}\n\n"
  fi
}

main
