"""Deprecated compatibility wrapper to legacy simulator."""

from src.legacy.simulator import main


if __name__ == "__main__":
    raise SystemExit(main())
