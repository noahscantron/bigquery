CREATE TABLE dataset.table (
   last_modified_at TIMESTAMP
  ,internal_id      STRING
  ,psku             STRING
  ,sku              STRING
  ,old_value        NUMERIC
  ,new_value        NUMERIC
  ,field            STRING
  ,price_level      STRING
  ,type             STRING
  ,set_by           STRING
  ,context          STRING
  ,ingested_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE_TRUNC(last_modified_at, DAY)
CLUSTER BY psku;