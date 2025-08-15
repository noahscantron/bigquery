/*
This creates a table with the same:
  - data
  - schema
  - partitioning
  - clustering
  - everything

... as the COPY table
*/

CREATE TABLE dataset.copy_table
COPY         dataset.target_table