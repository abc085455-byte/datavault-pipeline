from scripts.load.snowflake_loader import SnowflakeLoader

s = SnowflakeLoader()

print("--- Snowflake Connection Test ---")
s.connect()
print("✅ Connected!")

# Test query chalao
cursor = s.conn.cursor()
cursor.execute("SELECT CURRENT_TIMESTAMP(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
row = cursor.fetchone()
print(f"✅ Time     : {row[0]}")
print(f"✅ Database : {row[1]}")
print(f"✅ Schema   : {row[2]}")
cursor.close()

s.disconnect()
print("✅ Disconnected!")