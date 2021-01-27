import sys
import argparse
from csv import reader

import boto3
from tqdm import tqdm

parser = argparse.ArgumentParser(
    description="Import CSV to DynamoDB Table. CSV headers must map to DynamoDB Table field names."
)
parser.add_argument("csvFile", help="Path to CSV file")
parser.add_argument("tableName", help="DynamoDB Table name")
parser.add_argument("writeRate", default=0, type=int, nargs="?", help="Number of writes to table per second (default:0 - on-demand)")
parser.add_argument("delimiter", default=",", nargs="?", help="Delimiter for CSV file (default=,)")
args = parser.parse_args()


def snake_to_camel_case(snake_string):
    if "_" not in snake_string:
        return snake_string

    pascal = snake_string.replace("_", " ").title().replace(" ", "")
    return pascal[0].lower() + pascal[1:]


def main():
    print("Your arguments:", args.__dict__)

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(args.tableName)

    with open(args.csvFile) as csv_file:
        tokens = reader(csv_file, delimiter=args.delimiter)
        # read first line in file (header)
        headers = next(tokens)
        headers = [snake_to_camel_case(header) for header in headers]
        print("Extracted Headers from CSV:", headers)

        # rest is data
        full_record_length = 0
        bulk_item = []

        for token in tokens:
            full_record_length += 1
            item = {}
            for i, val in enumerate(token):
                if val:
                    key = headers[i]
                    item[key] = val
            if item:
                bulk_item.append(item)

        print(f"Found {full_record_length} rows and {len(bulk_item)} were valid.")
        print(f"Memory size for the records is {sys.getsizeof(bulk_item)} bytes.")
        if input("Execute import? (Y/N) : ") != "Y":
            print("Aborted!")
            return

        with table.batch_writer() as writer:
            for item in tqdm(bulk_item):
                writer.put_item(Item=item)


if __name__ == "__main__":
    main()