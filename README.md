# LittleBigQuery

LittleBigQuery is a Python module to simplify access to [Google Big Query](bigquery.cloud.google.com).  It provides:

  - Database-style commands for
    - Creating and dropping tables
    - Appending data
    - Describing tables and partitions
  - Results as [Pandas](pandas.pydata.org) DataFrames
  - Time-partitioning of existing tables with a TIMESTAMP column

Usage:

`from little_big_query import *`

`bq = LittleBigQuery(<yourProjectId>, <datasetId>)`

`panda_frame = bq.query("SELECT emp FROM dep;")`

`bq.createTableFromCSV("myTable", 
    [("col1","INTEGER"), ("col2", "STRING)],
    "gs://myBucket/myDirectory/*")`

`bq.desc("myTable")`
`
