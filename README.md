# csv-dynamo-importer

A script to import CSV data into DynamoDB.

## Usage

### 1. Install dependencies

```bash
pipenv install                    # if you have pipenv installed
pip3 install -r requirements.txt  # otherwise
```

### 2. Prerequisites

* Create a table in DynamoDB with proper hash-key and range-key.
* Set proper Provisioned Throughput (or on-demand).
* Check your CSV headers.

> `snake_case` header names will be automatically converted to `camelCase`.

### 3. Run script

```bash
python3 main.py sample.csv table-name
```

### Arguments

```
> python3 main.py -h                                                     
usage: main.py [-h] csvFile tableName [writeRate] [delimiter]

Import CSV to DynamoDB Table. CSV headers must be identical to DynamoDB Table field names (at least for hash-key and range-key).

positional arguments:
  csvFile     Path to CSV file
  tableName   DynamoDB Table name
  writeRate   Number of writes to table per second (default:0 - on-demand)
  delimiter   Delimiter for CSV file (default=,)

optional arguments:
  -h, --help  show this help message and exit

```

## Example

```text
Your arguments: 
╭─────────┬──────────────────────────────────╮
│csvFile  │frooto-frooto-sample.csv          │
│tableName│pregnancy-tracker-server-qa-Frooto│
│writeRate│0                                 │
│delimiter│,                                 │
╰─────────┴──────────────────────────────────╯
Extracted headers from CSV: 
╭──┬──────────────────╮
│0 │id                │
│1 │createdAt         │
│2 │creator           │
│3 │deletedAt         │
│4 │updateRequestId   │
│5 │updatedAt         │
│6 │updater           │
│7 │balance           │
│8 │currency          │
│9 │fee               │
│10│firstDepositAt    │
│11│tradable          │
│12│type              │
│13│userId            │
│14│withdrawalDeferred│
│15│version           │
╰──┴──────────────────╯
Found 1000 row(s) and 1000 were valid.
Memory size for the records is 8856 bytes.
Split data into 1MB chunks...
Splitting: 100%|███████████████████████████████████| 1000/1000 [00:00<00:00, 224270.35it/s]
Created 1 chunk(s) with 1MB data. Totally containing 1000 record(s). Lost 0 record(s).
Execute import? (Y/N) : Y
Upload starts...
Items in chunk: 100%|█████████████████████████████████| 1000/1000 [00:01<00:00, 537.98it/s]
Chunks: 100%|████████████████████████████████████████████████| 1/1 [00:01<00:00,  1.86s/it]
Now we scan the whole table to verify the result...
Full scan length is 1000.
Done! Goodbye!
```

## Author

Kevin Kim
