from log import logger

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

        with self._table.batch_writer() as writer:
            for i, item in tqdm(enumerate(bulk_item), desc="Uploading", initial=begins, total=ends):
                logger.info(begins + i + 1)
                writer.put_item(Item=item)

    def count_all_rows(self):
        full_scan_length = 0
        done = False
        start_key = None
        scan_kwargs = {
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
