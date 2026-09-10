# Create the virtualenv and install threadmill in editable mode.
py=""
for c in python3.12 python3.13 python3; do
  have "$c" && py="$c" && break
done
[ -n "$py" ] || fail "Python 3.12+ not found" "run: brew install python@3.12"
"$py" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 12) else 1)' \
  || fail "$py is older than 3.12" "run: brew install python@3.12"

[ -d .venv ] || quiet "$py" -m venv .venv || fail "could not create .venv"
quiet .venv/bin/python -m pip install -e . || fail "pip install failed"
ok "installed threadmill into .venv"
