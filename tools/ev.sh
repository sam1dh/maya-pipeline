#!/usr/bin/env bash
# Show the latest important pipeline events (mode switches, restarts, sessions, losses).
journalctl -u maya-pipeline -n "${1:-30}" --no-pager | grep -E "running|MODE|new mock|loss|catching"
