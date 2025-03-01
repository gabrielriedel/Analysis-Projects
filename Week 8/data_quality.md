# Data Parsing and Quality Issues

## Initial Problems

The first parsing issue I ran into was when compressing my csv into a parquet, missing values were being recorded as N/A, "", and null causing the columns to have multiple data types. To remedy this, I mapped all of these values in the csv to NaN. 

## Missing Data Galore
