## Summary
This project combines song listen log files with song metadata to facilitate analytics. A Redshift cluster is created using the Python SDK and a data pipeline built in Python and SQL prepares a data schema designed for analytics. 
## Files

**`create_cluster.py`**

**`create_tables.py`** 

**`dwh.cfg`** 

**`etl.py`**

**`sql_queries.py`**

## Run scripts

```bash
$ python create_cluster.py

```bash
$ python create_tables.py
```
```bash
$ python etl.py
```

Delete IAM role and Redshift cluster
$ python create_cluster.py --delete
```
