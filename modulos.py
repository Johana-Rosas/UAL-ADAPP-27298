from rapidfuzz import process, fuzz
import mysql.connector
import pandas as pd


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
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
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
    conn = connect_to_mysql(
        host=params_dict.get("host", "localhost"),
        username=params_dict.get("username", "root"),
        password=params_dict.get("password", ""),
        port=params_dict.get("port", 3306),
        database=None
    )

    ensure_databases_exist(conn, ["dbo", "crm"])

    cursor = conn.cursor()

    if 'src_dest_mappings' not in params_dict or not params_dict['src_dest_mappings']:
        raise ValueError("Debe proporcionar src_dest_mappings con columnas origen y destino")

    src_cols = ", ".join(params_dict['src_dest_mappings'].keys())
    dest_cols = ", ".join(params_dict['src_dest_mappings'].values())

    sql_source = f"SELECT {src_cols} FROM {params_dict['sourceTable']}"
    sql_dest   = f"SELECT {dest_cols} FROM {params_dict['destTable']}"

    cursor.execute(sql_source)
    src_rows = cursor.fetchall()
    src_columns = [desc[0] for desc in cursor.description]
    source_data = [dict(zip(src_columns, row)) for row in src_rows]

    cursor.execute(sql_dest)
    dest_rows = cursor.fetchall()
    dest_columns = [desc[0] for desc in cursor.description]
    dest_data = [dict(zip(dest_columns, row)) for row in dest_rows]

    conn.close()

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
