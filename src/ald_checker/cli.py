"""CLI entry point for ald-check."""
from __future__ import annotations

import argparse
import sys

from ald_checker.checks import run_checks, ALL_CHECKS


def main():
    # Build check names for --only/--skip
    check_names = [fn.__name__.replace("check_", "") for fn in ALL_CHECKS]

    parser = argparse.ArgumentParser(
        prog="ald-check",
        description="ALD output quality checker — validates and fixes asset location CSVs",
    )
    parser.add_argument("files", nargs="+", help="CSV file(s) to check")
    parser.add_argument("--fix", action="store_true",
                        help="Deterministic fixes: IDs, casing, dates, columns, JSON, etc.")
    parser.add_argument("--fix-llm", action="store_true",
                        help="LLM fixes: raw types, NatureSense, GICS, status, attribution")
    parser.add_argument("--model", default="",
                        help="LLM model for --fix-llm (default: from config or openai/gpt-4.1-nano)")
    parser.add_argument("--only", nargs="+", metavar="CHECK",
                        help=f"Run only these checks. Available: {', '.join(check_names)}")
    parser.add_argument("--skip", nargs="+", metavar="CHECK",
                        help="Skip these checks")
    parser.add_argument("--no-xlsx", action="store_true",
                        help="Skip xlsx output generation")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be fixed without writing output")
    args = parser.parse_args()

    fix = args.fix or args.fix_llm
    all_passed = True
    for f in args.files:
        results = run_checks(
            f,
            fix=fix,
            fix_llm=args.fix_llm,
            model=args.model,
            only_checks=args.only,
            skip_checks=args.skip,
            no_xlsx=args.no_xlsx,
            dry_run=args.dry_run,
        )
        if any(not r.passed and not r.fixed for r in results):
            all_passed = False

    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
