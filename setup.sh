#!/usr/bin/env bash
cd "$(dirname "$0")"
source scripts/common.sh

step "1. Install tools"
source scripts/homebrew.sh

step "2. Python environment"
source scripts/python.sh

step "Setup complete"
echo "  run: source .venv/bin/activate && python demo.py"
