import duckdb as dd



csv_path = '/Users/gaberiedel/Downloads/2022_place_canvas_history.csv'  

conn = dd.connect('place_database.db')

conn.execute(f"CREATE TABLE my_table AS SELECT * FROM read_csv_auto('{csv_path}')")

conn.close()




