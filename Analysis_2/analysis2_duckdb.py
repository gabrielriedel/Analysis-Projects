import duckdb as dd
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

conn = dd.connect('place_database.db')

query1 = """
    SELECT pixel_color, COUNT(*) AS color_count
    FROM my_table 
    WHERE timestamp BETWEEN ? AND ?
    GROUP BY pixel_color
    ORDER BY color_count DESC
    LIMIT 1"""

query2 = """
    SELECT coordinate, COUNT(*) AS coordinate_count
    FROM my_table
    WHERE timestamp BETWEEN ? AND ?
    GROUP BY coordinate
    ORDER BY coordinate_count DESC
    LIMIT 1;
    """

results1 = conn.execute(query1, (start_time, end_time)).fetchall()

results2 = conn.execute(query2, (start_time, end_time)).fetchall()

end_time_ns = time.perf_counter_ns()
elapsed_time_ms = (end_time_ns - start_time_ns) / 1e6
print("Reult of query 1", results1)
print("Reult of query 2", results2)
print(elapsed_time_ms)

conn.close()



