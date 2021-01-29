import argparse
from csv import reader

from pyprnt import prnt


parser = argparse.ArgumentParser(
    description="Count rows in CSV file (excluding header)."
)
parser.add_argument("csvFile", help="Path to CSV file")

parser.add_argument("delimiter", default=",", nargs="?", help="Delimiter for CSV file (default=,)")
args = parser.parse_args()


def main():
    prnt("Your arguments:", args.__dict__)

    with open(args.csvFile) as csv_file:
        tokens = reader(csv_file, delimiter=args.delimiter)

        # read first line in file (header)
        _ = next(tokens)

        full_record_length = 0
        for _ in tokens:
            full_record_length += 1

        print(f"Full record length is {full_record_length}.")


if __name__ == "__main__":
    main()
