"""CLI runner for flow execution."""

from __future__ import annotations

import sys


def main() -> None:
    """Main entry point for the rinnsal CLI."""
    print("Usage: python your_script.py [options]")
    print("Run a flow script directly with --help for available options.")
    sys.exit(0)


if __name__ == "__main__":
    main()
