# Data Parsing and Quality Issues

## Initial Problems

My largest problem was getting the data loaded in the first place. I queried the data from a public api that I learned by some trial and error, I could only query about 1 GB at a time, and it took at least 20 minutes for each query. So I performed each query and wrote the data to a csv, and then concatenated all the csvs into one large csv. This process was computationally and time expensive. 

The first parsing issue I ran into was when compressing my csv into a parquet, missing values were being recorded as N/A, "", and null causing the columns to have multiple data types. To remedy this, I mapped all of these values in the csv to NaN. 

## Missing Data Galore

...will finish asap