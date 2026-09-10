# Shared setup helpers. Sourced by setup.sh and every scripts/*.sh.
# This file is identical across all repos; copy it verbatim when reusing.

set -euo pipefail

_log=$(mktemp)
trap 'rm -f "$_log"' EXIT
trap 'status=$?; [ $status -ne 0 ] && printf "\n  setup stopped (exit %s). fix the issue above and re-run ./setup.sh\n" "$status" >&2' ERR
export AWS_PAGER=""
ENV_FILE=".env"

_c() { [ -t 1 ] && printf '\033[%sm' "$1" || true; }
step() { printf '\n%s%s%s\n' "$(_c 1)" "$1" "$(_c 0)"; }
ok()   { printf '  %s✓%s %s\n' "$(_c 32)" "$(_c 0)" "$1"; }
info() { printf '  %s•%s %s\n' "$(_c 36)" "$(_c 0)" "$1"; }
warn() { printf '  %s!%s %s\n' "$(_c 33)" "$(_c 0)" "$1"; }
fail() {
  printf '  %s✗ %s%s\n' "$(_c 31)" "$1" "$(_c 0)"
  [ -n "${2:-}" ] && printf '    → %s\n' "$2"
  [ -s "$_log" ] && sed 's/^/    /' "$_log"
  exit 1
}

# Run a command, capturing output to $_log; clear the log on success.
quiet() { "$@" >"$_log" 2>&1 && : >"$_log"; }

have() { command -v "$1" >/dev/null 2>&1; }

confirm() {
  local answer
  read -rp "  $1 [y/N] " answer
  [ "$answer" = y ] || [ "$answer" = Y ]
}

# Read one value from .env, or nothing if absent.
env_get() { [ -f "$ENV_FILE" ] && sed -n "s/^$1=//p" "$ENV_FILE" | head -1 || true; }

# Insert or replace KEY=VALUE in .env (mode 600).
env_set() {
  touch "$ENV_FILE"
  if grep -q "^$1=" "$ENV_FILE" 2>/dev/null; then
    grep -v "^$1=" "$ENV_FILE" > "$ENV_FILE.tmp" && mv "$ENV_FILE.tmp" "$ENV_FILE"
  fi
  printf '%s=%s\n' "$1" "$2" >> "$ENV_FILE"
  chmod 600 "$ENV_FILE"
}

# prompt_secret KEY "human label" "url to obtain it"
# Uses .env, then an exported env var of the same name, then prompts.
prompt_secret() {
  local key=$1 label=$2 url=${3:-} value
  if [ -n "$(env_get "$key")" ]; then
    ok "$label already in $ENV_FILE"
    return
  fi
  if [ -n "${!key:-}" ]; then
    env_set "$key" "${!key}"
    ok "saved $key to $ENV_FILE from the environment"
    return
  fi
  info "$label"
  [ -n "$url" ] && info "get one here: $url"
  read -rsp "  paste $key: " value
  echo
  [ -n "$value" ] || fail "no value entered for $key"
  env_set "$key" "$value"
  ok "saved $key to $ENV_FILE"
}

# prompt_value KEY "human label" "default"  (visible, for non-secret config)
prompt_value() {
  local key=$1 label=$2 default=${3:-} value
  if [ -n "$(env_get "$key")" ]; then
    ok "$label already in $ENV_FILE"
    return
  fi
  read -rp "  $label${default:+ [$default]}: " value
  value=${value:-$default}
  [ -n "$value" ] || fail "no value entered for $key"
  env_set "$key" "$value"
  ok "saved $key to $ENV_FILE"
}

# Push one .env value to the repo as a GitHub Actions secret.
push_secret() {
  local key=$1 value
  value=$(env_get "$key")
  [ -n "$value" ] || fail "$key is missing from $ENV_FILE"
  printf '%s' "$value" | quiet gh secret set "$key" || fail "could not set GitHub secret $key"
  ok "GitHub secret $key set"
}

brew_bundle() {
  have brew || fail "Homebrew is not installed" "install it from https://brew.sh, then re-run ./setup.sh"
  quiet brew bundle --file Brewfile || fail "brew bundle failed"
  ok "installed the tools listed in Brewfile"
}
