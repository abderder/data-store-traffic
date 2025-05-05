import duckdb

conn = duckdb.connect("duckdb/local_db.duckdb")

# 🔍 Liste des tables
tables = conn.execute("SHOW TABLES").fetchall()
print("Tables dans la base :", tables)

# 🔎 Exemple : aperçu de la table 'visiteurs'
df = conn.execute("SELECT * FROM visiteurs LIMIT 5").fetchdf()
print("\nAperçu visiteurs :\n", df)

# 🔎 Exemple : aperçu de la table 'transactions'
df2 = conn.execute("SELECT * FROM transactions LIMIT 5").fetchdf()
print("\nAperçu transactions :\n", df2)

conn.close()
