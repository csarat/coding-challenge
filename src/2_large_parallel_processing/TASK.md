## Context
You are processing a large dataset containing millions of records. Each record must be enriched using a database lookup, which is simulated through a client class.

Important:
The database client is expensive to create, so it should be initialized only once per chunk (not per record and not shared globally across workers).


```
import time
import random

class DatabaseClient:
    def __init__(self):
        # expensive setup
        print("Initializing database connection...")
        time.sleep(0.1)

    def lookup(self, record):
        # lookup has latency
        time.sleep(0.005)
        return {
            "id": record["id"],
            "lookup_result": record["value"] * 10
        }

class DB:
    @staticmethod
    def client():
        return DatabaseClient()
```

## Your Task
Write a Python program that Splits the dataset into fixed-size chunks (e.g., 1000 records per chunk). Processes each chunk in parallel using multiple workers

For each chunk:
    * Processes all records in that chunk using .lookup(record)
    * Saves the output to output_chunk_{chunk_id}.json
    * Does not reuse the client across chunks or globally
    * Uses efficient parallelism 

## Constraints
* You may use only standard libraries

## Evaluation Criteria
* Correctness: Are all records processed and saved properly?
* Efficiency: Are chunks processed in parallel with correct use of the client?
* Code Structure: Is your code modular, readable, and logically organized?
* Resource Management: Are client instances initialized exactly once per chunk?