import duckdb

# Connexion à la base dans le dossier duckdb/
conn = duckdb.connect("duckdb/local_db.duckdb")

# Créer/importer la table visiteurs
conn.execute("""
    CREATE TABLE visiteurs AS
    SELECT * FROM read_csv_auto('data/visiteurs_03_2025.csv')
""")

# Créer/importer la table transactions
conn.execute("""
    CREATE TABLE transactions AS
    SELECT * FROM read_csv_auto('data/transactions_03_2025.csv')
""")

conn.close()
print("Base DuckDB remplie avec succès 🎉")
