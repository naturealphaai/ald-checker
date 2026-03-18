"""CLI entry point for ald-check."""
from __future__ import annotations

import argparse
import sys

from ald_checker.checks import run_checks


def main():
    parser = argparse.ArgumentParser(
        prog="ald-check",
        description="ALD output quality checker — validates and fixes asset location CSVs",
    )
    parser.add_argument("files", nargs="+", help="CSV file(s) to check")
    parser.add_argument("--fix", action="store_true",
                        help="Deterministic fixes: generate asset_ids, majority-vote "
                             "classifications, normalize statuses/casing, fill dates")
    parser.add_argument("--fix-llm", action="store_true",
                        help="LLM fixes for: invalid/ambiguous NatureSense types, "
                             "GICS codes, statuses. Requires: pip install ald-checker[llm]")
    parser.add_argument("--model", default="",
                        help="LLM model for --fix-llm (default: openai/gpt-4.1-nano)")
    args = parser.parse_args()

    fix = args.fix or args.fix_llm
    all_passed = True
    for f in args.files:
        results = run_checks(f, fix=fix, fix_llm=args.fix_llm, model=args.model)
        if any(not r.passed and not r.fixed for r in results):
            all_passed = False

    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
