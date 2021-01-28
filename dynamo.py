import boto3
from tqdm import tqdm


class Dynamo:
    def __init__(self, table_name):
        self._table_name = table_name
        self._table = self._initialize_table()

    def _initialize_table(self):
        dynamodb = boto3.resource("dynamodb")
        return dynamodb.Table(self._table_name)

    def batch_write(self, bulk_item, ends):
        begins = ends - len(bulk_item)

        print(f"Upload starts...")
        with self._table.batch_writer() as writer:
            for item in tqdm(bulk_item, desc="Items in chunk", initial=begins, total=ends):
                writer.put_item(Item=item)

    def count_all_rows(self):
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
            response = self._table.scan(**scan_kwargs)
            full_scan_length += len(response.get("Items"))
            start_key = response.get("LastEvaluatedKey", None)
            done = start_key is None
        return full_scan_length
