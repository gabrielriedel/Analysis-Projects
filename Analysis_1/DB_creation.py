import sqlite3
import csv

# Create table   
conn = sqlite3.connect('rplace_data.db')
cursor = conn.cursor()

cursor.execute('''
            CREATE TABLE IF NOT EXISTS place_data (
                timestamp TEXT,
                user_id TEXT,
                pixel_color TEXT,
                coordinate INTEGER
            )
        ''')


# Loading Data into table
filename = "2022_place_canvas_history.csv"
with open(filename, 'r') as file:
    reader = csv.DictReader(file)
    rows_to_insert = []
        
    batch_size = 100000
    for i, row in enumerate(reader, 1):
        rows_to_insert.append((row['timestamp'], row['user_id'], row['pixel_color'], row['coordinate']))
    
        if i % batch_size == 0:
            print(f"Processed {i} rows...")
            cursor.executemany('''
                INSERT INTO place_data (timestamp, user_id, pixel_color, coordinate)
                VALUES (?, ?, ?, ?)
            ''', rows_to_insert)
            conn.commit()
            rows_to_insert = []
    if rows_to_insert:
        cursor.executemany('''
                INSERT INTO place_data (timestamp, user_id, pixel_color, coordinate)
                VALUES (?, ?, ?, ?)
            ''', rows_to_insert)
        conn.commit()
        print(f"Inserted the last {len(rows_to_insert)} rows.")
    
conn.commit()
conn.close()