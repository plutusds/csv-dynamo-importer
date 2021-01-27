#!/usr/local/bin/python3

import sys
import argparse
from csv import reader

import boto3
from pyprnt import prnt
from tqdm import tqdm

parser = argparse.ArgumentParser(
    description="Import CSV to DynamoDB Table. CSV headers must be identical to DynamoDB Table field names (at least for hash-key and range-key)."
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
    prnt("Your arguments:", args.__dict__)

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(args.tableName)

    with open(args.csvFile) as csv_file:
        tokens = reader(csv_file, delimiter=args.delimiter)
        # read first line in file (header)
        headers = next(tokens)
        headers = [snake_to_camel_case(header) for header in headers]
        prnt("Extracted Headers from CSV:", headers)

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

        print(f"Found {full_record_length} row(s) and {len(bulk_item)} were valid.")
        assert full_record_length == len(bulk_item)

        print(f"Memory size for the records is {sys.getsizeof(bulk_item)} bytes.")

        print("Split data into 1MB chunks...")
        chunks = []
        lower = 0
        for upper in tqdm(range(len(bulk_item)), desc="Splitting"):
            size = sys.getsizeof(bulk_item[lower:upper])
            if size >= 1000000 or upper+1 == len(bulk_item):
                chunks.append(bulk_item[lower:upper+1])
                lower = upper+1

        chunks_count = sum([len(chunk) for chunk in chunks])
        print(f"Created {len(chunks)} chunk(s) with 1MB data. Totally containing {chunks_count} record(s). Lost {full_record_length-chunks_count} record(s).")

        if input("Execute import? (Y/N) : ") != "Y":
            print("Aborted!")
            return

        print(f"Upload starts...")
        with table.batch_writer() as writer:
            for chunk in tqdm(chunks, desc="Chunks"):
                for item in tqdm(chunk, desc="Items in chunk"):
                    writer.put_item(Item=item)

        print("Now we scan the whole table to verify the result...")
        full_scan_length = 0
        done = False
        start_key = None
        scan_kwargs = {
            # 'FilterExpression': Key('year').between(*year_range),
            # 'ProjectionExpression': "#yr, title, info.rating",
            # 'ExpressionAttributeNames': {"#yr": "year"}
        }
        # Pagination with LastEvaluatedKey
        while not done:
            if start_key:
                scan_kwargs["ExclusiveStartKey"] = start_key
            response = table.scan(**scan_kwargs)
            full_scan_length += len(response.get("Items"))
            start_key = response.get("LastEvaluatedKey", None)
            done = start_key is None
        print(f"Full scan length is {full_scan_length}.")

        if full_record_length != full_scan_length:
            print("Something went wrong... Consider retry import.")
            return

        print(f"Done! Goodbye!")


if __name__ == "__main__":
    main()