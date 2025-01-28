import duckdb
import time
import sys
from datetime import datetime
import requests

file_path = "../data/2022big.parquet"

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

# Finding most frequent colors

result1 = duckdb.query(f"""SELECT pixel_color, COUNT(DISTINCT user_id) AS user_count
                        FROM '{file_path}'
                        WHERE timestamp BETWEEN '{start_time}' AND '{end_time}'
                        GROUP BY pixel_color
                        ORDER BY user_count DESC;""")
                      
df1 = result1.to_df()

def get_color_name(hex_color):
    url = f"https://www.thecolorapi.com/id?hex={hex_color.lstrip('#')}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return data.get("name", {}).get("value", "Unknown")
        else:
            return "Unknown"
    except Exception as e:
        return "Unknown"

df1['color_name'] = df1['pixel_color'].apply(get_color_name)

# Finding average session length

result2 = duckdb.query(f"""
    WITH user_activity AS (
        SELECT user_id, timestamp
        FROM '{file_path}'
        WHERE timestamp BETWEEN '{start_time}' AND '{end_time}'),
    sorted_activity AS (
        SELECT user_id, timestamp, LAG(timestamp) OVER (PARTITION BY user_id ORDER BY timestamp) AS prev_timestamp
        FROM user_activity
    ),
    sessions AS (
        SELECT user_id, timestamp,
            SUM(CASE WHEN (EXTRACT(EPOCH FROM timestamp) - EXTRACT(EPOCH FROM prev_timestamp)) > 900 
                    OR prev_timestamp IS NULL
                        THEN 1
                        ELSE 0
                END) OVER (PARTITION BY user_id ORDER BY timestamp) AS session_id
        FROM sorted_activity
    ),
    session_durations AS (
        SELECT user_id, session_id, MAX(EXTRACT(EPOCH FROM timestamp)) - MIN(EXTRACT(EPOCH FROM timestamp)) AS session_length
        FROM sessions
        GROUP BY user_id, session_id
    )
    SELECT AVG(session_length) AS avg_session_length
    FROM session_durations
    WHERE session_length > 0;
""")


df2 = result2.to_df()

# Finding percentiles of placed pixels

result3 = duckdb.query(f"""
    SELECT 
        quantile_cont(count, 0.50) AS p50,
        quantile_cont(count, 0.75) AS p75,
        quantile_cont(count, 0.90) AS p90,
        quantile_cont(count, 0.99) AS p99
    FROM (
        SELECT user_id, COUNT(*) AS count
        FROM '{file_path}'
        WHERE timestamp BETWEEN '{start_time}' AND '{end_time}'
        GROUP BY user_id
    ) subquery;
""")

df3 = result3.to_df()

# Counting first time users

result4 = duckdb.query(f"""
    SELECT COUNT(*) as first_user_count
    FROM (
        SELECT user_id, MIN(timestamp) AS first_timestamp
        FROM '{file_path}'
        GROUP BY user_id
    ) subquery
    WHERE first_timestamp BETWEEN '{start_time}' AND '{end_time}';
""")

df4 = result4.to_df()


print(df1)
print(df2)
print(df3)
print(df4)
end_time_ns = time.perf_counter_ns()
elapsed_time_ms = (end_time_ns - start_time_ns) / 1e6
print(elapsed_time_ms)
