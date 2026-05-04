#!/usr/bin/env bash
# Run unit tests with coverage and open the HTML report.
set -euo pipefail

uv run pytest tests/unit -q --timeout=10 \
    --cov --cov-branch --cov-report=term-missing --cov-report=html "$@"

echo ""
echo "HTML report: htmlcov/index.html"

# Open in browser if possible
if command -v open &>/dev/null; then
    open htmlcov/index.html
elif command -v xdg-open &>/dev/null; then
    xdg-open htmlcov/index.html
fi
