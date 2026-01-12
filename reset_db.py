import sqlite3

DB_PATH = "pisco_inventory.db"

conn = sqlite3.connect(DB_PATH)

print("BEFORE:")
print(conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall())

conn.execute("DROP TABLE IF EXISTS inventory_data")
conn.execute("DROP TABLE IF EXISTS detect_total_audit")

conn.commit()

print("AFTER:")
print(conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall())

conn.close()

print("✅ DB RESET DONE")
