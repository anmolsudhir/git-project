import sys
import argparse


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
        choices=["sql", "parquet", "csv"],
        default="parquet",
        help="Export format for the extracted commit info.",
    )

    # Parse arguments
    args = extractor.parse_args()

    print(args)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(1)
