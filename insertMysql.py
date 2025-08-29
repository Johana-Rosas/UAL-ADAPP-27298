import mysql.connector
import csv

# 🔹 Configuración centralizada CAMBIO 1
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "fer0110_*"  # <-- ajusta tu contraseña
}


def connect_to_db(database):
    return mysql.connector.connect(**DB_CONFIG, database=database)


def insert_from_csv(csv_file, conn, table, columns, update_cols):
    """
    Inserta/actualiza datos desde un CSV en la tabla indicada.
    """
    cursor = conn.cursor()

    # Construcción dinámica de placeholders (%s)
    placeholders = ", ".join(["%s"] * len(columns))
    col_names = ", ".join(columns)

    # Construcción de UPDATE dinámico
    update_clause = ", ".join([f"{col}=VALUES({col})" for col in update_cols])

    sql = f"""
        INSERT INTO {table} ({col_names})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_clause}
    """

    # Leer CSV en memoria
    with open(csv_file, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        valores = [tuple(row[col] for col in columns) for row in reader]

    # Ejecutar en lote CAMBIO 2
    cursor.executemany(sql, valores)
    conn.commit()
    print(f"✅ {len(valores)} registros insertados/actualizados en {table}")


# === Uso ===
if __name__ == "__main__":
    # Conexiones
    conn_dbo = connect_to_db("dbo")
    conn_crm = connect_to_db("crm")

    # Insertar usuarios
    insert_from_csv(
        "usuarios.csv",
        conn_dbo,
        table="Usuarios",
        columns=["username", "first_name", "last_name", "email", "password_hash", "rol"],
        update_cols=["first_name", "last_name", "email", "password_hash", "rol"]
    )

    # Insertar clientes
    insert_from_csv(
        "clientes.csv",
        conn_crm,
        table="Clientes",
        columns=["nombre", "apellido", "email"],
        update_cols=["nombre", "apellido"]
    )

    # Cerrar conexiones
    conn_dbo.close()
    conn_crm.close()
