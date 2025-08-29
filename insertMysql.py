import mysql.connector
import csv

# Conexión a dbo (Usuarios)
conn_dbo = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",   # 🔹 pon tu contraseña si tienes
    database="dbo"
)
cursor_dbo = conn_dbo.cursor()

# Conexión a crm (Clientes)
conn_crm = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",   # 🔹 pon tu contraseña si tienes
    database="crm"
)
cursor_crm = conn_crm.cursor()


def insert_usuarios(csv_file):
    with open(csv_file, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sql = """
            INSERT INTO Usuarios (username, first_name, last_name, email, password_hash, rol)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                first_name = VALUES(first_name),
                last_name = VALUES(last_name),
                email = VALUES(email),
                password_hash = VALUES(password_hash),
                rol = VALUES(rol)
            """
            valores = (
                row["username"],
                row["first_name"],
                row["last_name"],
                row["email"],
                row["password_hash"],
                row["rol"]
            )
            cursor_dbo.execute(sql, valores)
    conn_dbo.commit()
    print("✅ Usuarios insertados/actualizados en dbo.Usuarios")


def insert_clientes(csv_file):
    with open(csv_file, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sql = """
            INSERT INTO Clientes (nombre, apellido, email)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                nombre = VALUES(nombre),
                apellido = VALUES(apellido)
            """
            valores = (
                row["nombre"],
                row["apellido"],
                row["email"]
            )
            cursor_crm.execute(sql, valores)
    conn_crm.commit()
    print("✅ Clientes insertados/actualizados en crm.Clientes")


# Ejecutar funciones
insert_usuarios("usuarios.csv")
insert_clientes("clientes.csv")

# Cerrar cursores y conexiones
cursor_dbo.close()
conn_dbo.close()
cursor_crm.close()
conn_crm.close()
