"""CLI entry point."""

from __future__ import annotations

from .args import create_argument_parser
from .runner import run_from_parsed_args


def main() -> int:
    """Main entry point for CLI execution."""
    from dotenv import load_dotenv

    load_dotenv()

    parser = create_argument_parser()
    args = parser.parse_args()
    return run_from_parsed_args(parser, args)


if __name__ == "__main__":
    raise SystemExit(main())
