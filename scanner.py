import argparse

from pyprnt import prnt

from dynamo import Dynamo


parser = argparse.ArgumentParser(
    description="Scan DynamoDB Table and return length."
)
parser.add_argument("tableName", help="DynamoDB Table name")
args = parser.parse_args()


def main():
    prnt("Your arguments:", args.__dict__)

    dynamo = Dynamo(args.tableName)

    counts = dynamo.count_all_rows()
    print(counts)


if __name__ == "__main__":
    main()
