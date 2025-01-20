import polars as pl
import time
import sys
from datetime import datetime

start_time_ns = time.perf_counter_ns()

start = sys.argv[1]
end = sys.argv[2]

try:
    start_time = datetime.strptime(start, "%Y-%m-%d %H")
    end_time = datetime.strptime(end, "%Y-%m-%d %H")
except:
    print("Start and end dates must be in the format YYYY-MM-DD HH")
    exit(1)

if start_time > end_time:
    print("Start time must occur before end time")
    exit(1)


lazy_df = pl.scan_csv("/Users/gaberiedel/Downloads/2022_place_canvas_history.csv")

results1 = (lazy_df.filter((pl.col("timestamp") > start) & (pl.col("timestamp") < end)) 
      .group_by("pixel_color") 
      .agg(pl.col("pixel_color").count().alias("color_count"))
      .sort("color_count", descending=True)
      .head(1) 
).collect()

results2 = (lazy_df.filter((pl.col("timestamp") > start) & (pl.col("timestamp") < end)) 
      .group_by("coordinate") 
      .agg(pl.col("coordinate").count().alias("coordinate_count"))
      .sort("coordinate_count", descending=True)
      .head(1) 
).collect()

end_time_ns = time.perf_counter_ns()
elapsed_time_ms = (end_time_ns - start_time_ns) / 1e6
print("Reult of query 1", results1)
print("Reult of query 2", results2)
print("Elapsed Time: ", elapsed_time_ms)

