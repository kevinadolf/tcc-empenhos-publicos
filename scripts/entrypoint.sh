#!/bin/sh
set -e

COMMAND="$1"
shift || true

if [ "$COMMAND" = "cli" ]; then
    exec python -m src.backend.cli "$@"
fi

# Default to running backend API
if [ -z "$COMMAND" ] || [ "$COMMAND" = "serve" ]; then
    exec python -m src.backend.app "$@"
fi

# Fallback: run arbitrary command
exec "$COMMAND" "$@"
