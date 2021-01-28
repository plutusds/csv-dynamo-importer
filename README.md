# csv-dynamo-importer

A fault tolerant script to import large volume CSV data into DynamoDB.

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
usage: main.py [-h] csvFile tableName [offset] [writeRate] [delimiter]

Import CSV to DynamoDB Table. CSV headers must be identical to DynamoDB Table field names (at least for hash-key and range-key).

positional arguments:
  csvFile     Path to CSV file
  tableName   DynamoDB Table name
  offset      Offset index (default:0)
  chunkSize   Chunk size (default:10MB)
  writeRate   Number of writes to table per second (default:0 - on-demand)
  delimiter   Delimiter for CSV file (default=,)

optional arguments:
  -h, --help  show this help message and exit
```

## Example

```text
> p main.py frooto-sample.csv Wallet 0 1KB
Your arguments: 
╭─────────┬──────────────────────────────────╮
│csvFile  │frooto-sample.csv                 │
│tableName│Wallet                            │
│offset   │0                                 │
│chunkSize│1KB                               │
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
Read only 1KB chunks
Reached size above 1KB (1000). Current size is 1080 and length is 109.
Uploading: 100%|███████████████████████████████████████████████████████████████████████| 109/109 [00:01<00:00, 73.37it/s]
Reached size above 1KB (1000). Current size is 1080 and length is 109.
Uploading: 100%|██████████████████████████████████████████████████████████████████████| 218/218 [00:00<00:00, 286.29it/s]
Reached size above 1KB (1000). Current size is 1080 and length is 109.
Uploading: 100%|██████████████████████████████████████████████████████████████████████| 327/327 [00:00<00:00, 850.98it/s]
Reached size above 1KB (1000). Current size is 1080 and length is 109.
Uploading: 100%|██████████████████████████████████████████████████████████████████████| 436/436 [00:00<00:00, 859.11it/s]
Reached size above 1KB (1000). Current size is 1080 and length is 109.
Uploading: 100%|██████████████████████████████████████████████████████████████████████| 545/545 [00:00<00:00, 820.43it/s]
Reached size above 1KB (1000). Current size is 1080 and length is 109.
Uploading: 100%|██████████████████████████████████████████████████████████████████████| 654/654 [00:00<00:00, 906.86it/s]
Reached size above 1KB (1000). Current size is 1080 and length is 109.
Uploading: 100%|██████████████████████████████████████████████████████████████████████| 763/763 [00:00<00:00, 766.63it/s]
Reached size above 1KB (1000). Current size is 1080 and length is 109.
Uploading: 100%|██████████████████████████████████████████████████████████████████████| 872/872 [00:00<00:00, 829.78it/s]
Reached size above 1KB (1000). Current size is 1080 and length is 109.
Uploading: 100%|██████████████████████████████████████████████████████████████████████| 981/981 [00:00<00:00, 826.64it/s]
Remaining size is 248 and length is 19.
Uploading: 100%|█████████████████████████████████████████████████████████████████| 1000/1000 [00:00<00:00, 182779.30it/s]
Now we scan the whole table to verify the result...
Full scan length is 1000.
Done! Goodbye!
```

## Author

Kevin Kim
