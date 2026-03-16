"""Rinnsal log viewer - a web-based experiment dashboard.

Install with: pip install rinnsal[viewer]
Run with: rinnsal-viewer /path/to/runs
"""


def main():
    """CLI entry point for the viewer."""
    try:
        from rinnsal.viewer.app import main as _main
    except ImportError:
        print(
            "Viewer dependencies not installed. "
            "Install with: pip install rinnsal[viewer]"
        )
        raise SystemExit(1)
    _main()
