# Data Parsing and Quality Issues

## Initial Problems

My largest problem was getting the data loaded in the first place. I queried the data from a public api that I learned by some trial and error, I could only query about 1 GB at a time, and it took at least 20 minutes for each query. So I performed each query and wrote the data to a csv, and then concatenated all the csvs into one large csv. This process was computationally and time expensive. 


## Missing Data Galore

The main parsing issue I ran into was when compressing my csv into a parquet, missing values were being recorded as N/A, "", and null causing the columns to have multiple data types. To remedy this, I mapped all of these values in the csv to NaN. However, even after this, no matter what method I used to compress the csv to parquet, a large portion of the columns had mismatched types because of the concatenation. I ended up not compressing to a parquet.
Fortunately (or unfortunately), sticking with the large CSV did not end up to be an issue because my question is focused on launch angles, and launch angles did not start being tracked until 2015, so I ended up only querying a small subset of my data that dated back to the 1960s. In total I have about 2 million rows of data without missing launch angles to operate on.