import sys
import argparse
from csv import reader

from pyprnt import prnt

from dynamo import Dynamo


parser = argparse.ArgumentParser(
    description="Import CSV to DynamoDB Table. CSV headers must be identical to DynamoDB Table field names (at least for hash-key and range-key)."
)
parser.add_argument("csvFile", help="Path to CSV file")
parser.add_argument("tableName", help="DynamoDB Table name")
parser.add_argument("offset", default=0, type=int, nargs="?", help="Offset index (default:0)")
parser.add_argument("chunkSize", default="10MB", nargs="?", help="Chunk size (default:10MB)")
parser.add_argument("writeRate", default=0, type=int, nargs="?", help="Number of writes to table per second (default:0 - on-demand)")
parser.add_argument("delimiter", default=",", nargs="?", help="Delimiter for CSV file (default=,)")
args = parser.parse_args()


def size_converter(size: str):
    if not size.endswith(("KB", "MB")):
        raise ValueError("Invalid chunk size")

    if size.endswith("KB"):
        multiplier = 1000
    else:
        multiplier = 1000000

    size = int(size.replace("KB", "").replace("MB", ""))
    return size * multiplier


def snake_to_camel_case(snake_string):
    if "_" not in snake_string:
        return snake_string

    pascal = snake_string.replace("_", " ").title().replace(" ", "")
    return pascal[0].lower() + pascal[1:]


def main():
    prnt("Your arguments:", args.__dict__)

    max_chunk_size_string = args.chunkSize
    max_chunk_size = size_converter(max_chunk_size_string)

    dynamo = Dynamo(args.tableName)

    with open(args.csvFile) as csv_file:
        tokens = reader(csv_file, delimiter=args.delimiter)
        # read first line in file (header)
        headers = next(tokens)
        headers = [snake_to_camel_case(header) for header in headers]
        prnt("Extracted headers from CSV:", headers)

        # rest is data
        full_record_length = 0
        bulk_item = []

        print(f"Read only {max_chunk_size_string} chunks")
        for token in tokens:
            full_record_length += 1
            if full_record_length <= args.offset:
                continue

            item = {}
            for i, val in enumerate(token):
                if val:
                    key = headers[i]
                    item[key] = val
            if item:
                bulk_item.append(item)

            size = sys.getsizeof(bulk_item)
            if size >= max_chunk_size:
                print(f"Reached size above {max_chunk_size_string} ({max_chunk_size}). Current size is {size} and length is {len(bulk_item)}.")
                dynamo.batch_write(bulk_item, full_record_length)
                bulk_item = []
        if bulk_item:
            # still have items in bulk
            size = sys.getsizeof(bulk_item)
            print(f"Remaining size is {size} and length is {len(bulk_item)}.")
            dynamo.batch_write(bulk_item, full_record_length)

        print("Now we scan the whole table to verify the result...")
        full_scan_length = dynamo.count_all_rows()
        print(f"Full scan length is {full_scan_length}.")

        if full_record_length != full_scan_length:
            print("Something went wrong... Consider retry import.")
            return

        print(f"Done! Goodbye!")


if __name__ == "__main__":
    main()