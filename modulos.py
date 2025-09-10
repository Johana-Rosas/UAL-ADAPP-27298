from rapidfuzz import process, fuzz
import mysql.connector
import pandas as pd
import os


def connect_to_mysql(host, username, password, port=3306, database=None):
    connection = mysql.connector.connect(
        host=host,
        user=username,
        password=password,
        port=port,
        database=database if database else None
    )
    return connection


def ensure_databases_exist(conn, databases):
    cursor = conn.cursor()
    for db in databases:
        cursor.callproc('sp_create_database_if_not_exists_27298', (db,))
    conn.commit()
    cursor.close()


def fuzzy_match(queryRecord, choices, score_cutoff=0):
    scorers = [fuzz.WRatio, fuzz.QRatio, fuzz.token_set_ratio, fuzz.ratio]
    processor = lambda x: str(x).lower()
    processed_query = processor(queryRecord)
    choices_data = []

    for choice in choices:
        dict_choices = dict(choice)
        queryMatch = ""
        dict_match_records = {}
        for k, v in dict_choices.items():
            if k != "DestRecordId":
                val = str(v) if v is not None else ""
                queryMatch += val
                dict_match_records[k] = v

        choices_data.append({
            'query_match': queryMatch,
            'dest_record_id': dict_choices.get('DestRecordId'),
            'match_record_values': dict_match_records
        })

    all_matches = []

    for scorer in scorers:
        results = process.extract(
            query=processed_query,
            choices=[item['query_match'] for item in choices_data],
            scorer=scorer,
            score_cutoff=score_cutoff,
            processor=processor
        )

        for match_value, score, index in results:
            matched_item = choices_data[index]
            all_matches.append({
                'match_query': queryRecord,
                'match_result': match_value,
                'score': score,
                'match_result_values': matched_item['match_record_values']
            })

    return all_matches


def execute_dynamic_matching(params_dict, score_cutoff=0):
    # Conexión a base de datos de origen
    conn_src = connect_to_mysql(
        host=params_dict.get("host", "localhost"),
        username=params_dict.get("username", "root"),
        password=params_dict.get("password", ""),
        port=params_dict.get("port", 3306),
        database="dbo"
    )
    # Conexión a base de datos de destino
    conn_dest = connect_to_mysql(
        host=params_dict.get("host", "localhost"),
        username=params_dict.get("username", "root"),
        password=params_dict.get("password", ""),
        port=params_dict.get("port", 3306),
        database="crm"
    )

    ensure_databases_exist(conn_src, ["dbo"])
    ensure_databases_exist(conn_dest, ["crm"])

    cursor_src = conn_src.cursor()
    cursor_dest = conn_dest.cursor()

    if 'src_dest_mappings' not in params_dict or not params_dict['src_dest_mappings']:
        raise ValueError("Debe proporcionar src_dest_mappings con columnas origen y destino")

    src_cols = ", ".join(params_dict['src_dest_mappings'].keys())
    dest_cols = ", ".join(params_dict['src_dest_mappings'].values())

    sql_source = f"SELECT {src_cols} FROM {params_dict['sourceTable']}"
    sql_dest   = f"SELECT {dest_cols} FROM {params_dict['destTable']}"

    cursor_src.execute(sql_source)
    src_rows = cursor_src.fetchall()
    src_columns = [desc[0] for desc in cursor_src.description]
    source_data = [dict(zip(src_columns, row)) for row in src_rows]

    cursor_dest.execute(sql_dest)
    dest_rows = cursor_dest.fetchall()
    dest_columns = [desc[0] for desc in cursor_dest.description]
    dest_data = [dict(zip(dest_columns, row)) for row in dest_rows]

    conn_src.close()
    conn_dest.close()

    matching_records = []

    for record in source_data:
        dict_query_records = {}
        query = ""

        for src_col in params_dict['src_dest_mappings'].keys():
            val = record.get(src_col)
            query += str(val) if val is not None else ""
            dict_query_records[src_col] = val

        fm_list = fuzzy_match(query, dest_data, score_cutoff)

        for fm in fm_list:
            if fm["score"] > 0:  # Solo guarda si cumple el cutoff
                dict_query_records_copy = dict(dict_query_records)  # copia para no sobreescribir
                dict_query_records_copy.update(fm)
                dict_query_records_copy.update({
                    'destTable': params_dict['destTable'],
                    'sourceTable': params_dict['sourceTable']
                })
                matching_records.append(dict_query_records_copy)

    return matching_records




def display_results(results, as_dataframe=True):
    """
    Muestra los resultados como DataFrame o como lista de diccionarios.
    :param results: lista de diccionarios con los resultados
    :param as_dataframe: True -> DataFrame, False -> Diccionario
    """
    if as_dataframe:
        df = pd.DataFrame(results)
        print("\n=== Resultados en DataFrame ===")
        print(df)
        return df
    else:
        print("\n=== Resultados en Diccionario ===")
        for item in results:
            print(item)
        return results





def export_results_to_csv(results, filename="resultados.csv"):
    """
    Exporta los resultados a un archivo CSV.
    """
    if not results:
        print(" No hay resultados para exportar. El archivo CSV no fue creado.")
        return

    ensure_folder_exists(filename)

    import pandas as pd
    df = pd.DataFrame(results)
    df.to_csv(filename, index=False, encoding="utf-8")
    print(f" Resultados exportados correctamente a {filename}")

## Exportar resultados a Excel


#entregable 9 punto 2





def ensure_folder_exists(filepath):
    """Crea la carpeta destino si no existe"""
    folder = os.path.dirname(filepath)
    if folder and not os.path.exists(folder):
        os.makedirs(folder, exist_ok=True)






# entregable 10
def mostrar_columnas_disponibles(resultados):
    """
    Muestra los nombres de las columnas disponibles en los resultados.
    """
    if resultados:
        columnas = list(resultados[0].keys())
        print("\nColumnas disponibles para mostrar/exportar:")
        print(", ".join(columnas))
        return columnas
    else:
        print("No hay resultados para mostrar columnas.")
        return []

def filtrar_columnas(resultados, columnas_seleccionadas):
    """
    Filtra los resultados para mostrar solo las columnas seleccionadas.
    Siempre incluye la columna 'score' si existe.
    """
    if not resultados:
        return resultados

    # Asegura que 'score' esté en la lista de columnas
    columnas = set(columnas_seleccionadas)
    if 'score' in resultados[0]:
        columnas.add('score')

    filtrados = []
    for fila in resultados:
        filtrados.append({col: fila.get(col, "") for col in columnas})
    return filtrados

def renombrar_columnas(resultados, columnas_seleccionadas, nuevos_nombres):
    """
    Renombra las columnas seleccionadas según el diccionario nuevos_nombres.
    Siempre incluye la columna 'score' si existe.
    """
    if not resultados:
        return resultados

    columnas = set(columnas_seleccionadas)
    if 'score' in resultados[0]:
        columnas.add('score')

    filtrados = []
    for fila in resultados:
        nuevo_fila = {}
        for col in columnas:
            nuevo_nombre = nuevos_nombres.get(col, col)
            nuevo_fila[nuevo_nombre] = fila.get(col, "")
        filtrados.append(nuevo_fila)
    return filtrados

def preparar_resultados(resultados, columnas_seleccionadas, nuevos_nombres):
    """
    Prepara los resultados:
    - Convierte score a porcentaje.
    - Une nombre y apellido en 'full_name'.
    - Renombra columnas según nuevos_nombres.
    """
    if not resultados:
        return resultados

    columnas = set(columnas_seleccionadas)
    if 'score' in resultados[0]:
        columnas.add('score')
    # Siempre incluir full_name
    columnas.add('full_name')

    filtrados = []
    for fila in resultados:
        nuevo_fila = {}
        # Construir full_name
        nombre = fila.get('first_name') or fila.get('nombre') or ""
        apellido = fila.get('last_name') or fila.get('apellido') or ""
        full_name = f"{nombre} {apellido}".strip()
        # Score como porcentaje
        score = fila.get('score')
        score_pct = f"{round(score, 2)}%" if score is not None else ""
        for col in columnas:
            if col == 'score':
                nuevo_nombre = nuevos_nombres.get(col, col)
                nuevo_fila[nuevo_nombre] = score_pct
            elif col == 'full_name':
                nuevo_nombre = nuevos_nombres.get(col, col)
                nuevo_fila[nuevo_nombre] = full_name
            else:
                nuevo_nombre = nuevos_nombres.get(col, col)
                nuevo_fila[nuevo_nombre] = fila.get(col, "")
        filtrados.append(nuevo_fila)
    return filtrados

def export_results_to_excel(results, filename="resultados.xlsx"):
    """
    Exporta los resultados a un archivo Excel (.xlsx).
    """
    if not results:
        print(" No hay resultados para exportar. El archivo Excel no fue creado.")
        return

    # Crear carpeta si no existe
    ensure_folder_exists(filename)

    # Convertir resultados a DataFrame
    df = pd.DataFrame(results)

    # Exportar
    df.to_excel(filename, index=False, engine="openpyxl")
    print(f" Resultados exportados correctamente a {filename}")


    #entregable 11

def separar_matched_unmatched(resultados, score_col="score", threshold=97):
    """
    Separa los registros en matched y unmatched según el score.
    Un registro es matched si el score 
    """
    matched = []
    unmatched = []
    for fila in resultados:
        # El score puede venir como '97.0%' o como número
        score_val = fila.get(score_col, "")
        if isinstance(score_val, str) and score_val.endswith("%"):
            try:
                score_num = float(score_val.replace("%", ""))
            except ValueError:
                score_num = 0
        else:
            try:
                score_num = float(score_val)
            except (ValueError, TypeError):
                score_num = 0

        if score_num >= threshold:
            matched.append(fila)
        else:
            unmatched.append(fila)
    return matched, unmatched

def importar_archivo_y_insertar_tabla(filepath, db_params):
    """
    Importa un archivo CSV o Excel y lo inserta en la tabla 'matched_record'
    usando solo stored procedures, agregando control_number e inserted_at.
    """
    # Leer archivo
    if filepath.endswith('.csv'):
        df = pd.read_csv(filepath)
    elif filepath.endswith('.xlsx'):
        df = pd.read_excel(filepath)
    else:
        print("⚠️ Formato de archivo no soportado.")
        return

    # Agregar columnas de control si no existen
    if 'control_number' not in df.columns:
        df.insert(0, 'control_number', '')
    if 'inserted_at' not in df.columns:
        df['inserted_at'] = ''

    # Conectar a la base de datos
    conn = mysql.connector.connect(
        host=db_params.get("host", "localhost"),
        user=db_params.get("username", "root"),
        password=db_params.get("password", ""),
        port=db_params.get("port", 3306),
        database=db_params.get("database", "crm")
    )
    cursor = conn.cursor()

    # Borrar tabla usando stored procedure
    cursor.callproc('sp_drop_table_matched_record_27298')

    # Crear tabla usando stored procedure (agrega los campos extra)
    cols = ", ".join([f"`{col}` TEXT" for col in df.columns if col not in ['inserted_at']])
    cols += ", `inserted_at` DATETIME"
    cursor.callproc('sp_create_table_matched_record_27298', (cols,))

    # Obtener el último control_number
    cursor.execute("SELECT control_number FROM matched_record ORDER BY control_number DESC LIMIT 1")
    last_cn = cursor.fetchone()
    if last_cn and last_cn[0] and last_cn[0].startswith("DR"):
        last_seq = int(last_cn[0][2:])
    else:
        last_seq = 0

    # Insertar datos usando stored procedure
    col_names = ", ".join([f"`{col}`" for col in df.columns])
    now = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    for idx, row in df.iterrows():
        seq = last_seq + idx + 1
        control_number = f"DR{seq:04d}"
        values = [control_number] + [str(row[col]) if pd.notnull(row[col]) else "" for col in df.columns if col not in ['control_number', 'inserted_at']]
        values.append(now)
        values_sql = ", ".join([f"'{str(val).replace('\'', '\\\'')}'" if val != "" else "NULL" for val in values])
        cursor.callproc('sp_insert_file_matched_record_27298', (col_names, values_sql))
        print(f"Registro insertado: control_number={control_number}, inserted_at={now}")

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Datos importados e insertados en la tabla 'matched_record' usando stored procedures.")

def mostrar_matched_desde_db(db_params):
    """
    Lee y muestra los registros matched desde la tabla matched_record.
    """
    import mysql.connector
    import pandas as pd

    conn = mysql.connector.connect(
        host=db_params.get("host", "localhost"),
        user=db_params.get("username", "root"),
        password=db_params.get("password", ""),
        port=db_params.get("port", 3306),
        database=db_params.get("database", "crm")
    )
    df = pd.read_sql("SELECT * FROM matched_record", conn)
    conn.close()
    display_results(df.to_dict(orient="records"), as_dataframe=True)



