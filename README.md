# csv-dynamo-importer

A script to import CSV data into DynamoDB.

## Usage

### 1. Install dependencies

```bash
pipenv install                    # if you have pipenv installed
pip3 install -r requirements.txt  # otherwise
```

### 2. Run script

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
