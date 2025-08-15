/* Query to compare all rows between two tables */
(
  SELECT * FROM table_1
  EXCEPT DISTINCT
  SELECT * FROM table_2
)
UNION ALL
(
  SELECT * FROM table_2
  EXCEPT DISTINCT
  SELECT * FROM table_1
)