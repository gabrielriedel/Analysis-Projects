import pandas as pd
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


df = pd.read_csv("/Users/gaberiedel/Downloads/2022_place_canvas_history.csv")

results1 = (
    df[(df["timestamp"] > start) & (df["timestamp"] < end)]
    .groupby("pixel_color")
    .size()
    .reset_index(name="color_count")
    .sort_values("color_count", ascending=False)
    .head(1)
)

results2 = (
    df[(df["timestamp"] > start) & (df["timestamp"] < end)]
    .groupby("coordinate")
    .size()
    .reset_index(name="coordinate_count")
    .sort_values("coordinate_count", ascending=False)
    .head(1)
)

end_time_ns = time.perf_counter_ns()
elapsed_time_ms = (end_time_ns - start_time_ns) / 1e6
print("Reult of query 1", results1)
print("Reult of query 2", results2)
print("Elapsed Time: ", elapsed_time_ms)

