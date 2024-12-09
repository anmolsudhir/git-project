import sys
import argparse
from apps.extractor.utils import Extractor


def main():
    # Create argument parser
    extractor = argparse.ArgumentParser(
        prog="extractor",
        description="A simple cli tool to extract git commit info.",
        epilog="To exit press Ctrl+C.",
    )

    # Add boolean argument to to extract info only for today
    extractor.add_argument(
        "--today", action="store_true", help="Extract info only for today"
    )

    # Add the type to export commit info in
    extractor.add_argument(
        "--type",
        choices=["parquet", "csv"],
        default="parquet",
        help="Export format for the extracted commit info.",
    )

    # Parse arguments
    args = extractor.parse_args()

    extractor_tool: Extractor = Extractor(
        type=args.type,
        today=args.today,
        repo_url="git@github.com:watch1fy/lambda-backend.git",
    )
    print(extractor_tool.extract())


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(1)
