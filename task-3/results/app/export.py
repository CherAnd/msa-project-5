import os
import psycopg2
import csv

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

OUTPUT_PATH = "./shipments.csv"

def main():
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

    cur = conn.cursor()
    cur.execute("SELECT * FROM shipments")

    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()

    with open(OUTPUT_PATH, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(columns)
        writer.writerows(rows)

    cur.close()
    conn.close()

    print(f"Exported {len(rows)} rows to {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
