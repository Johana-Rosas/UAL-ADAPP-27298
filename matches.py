from rapidfuzz import process, fuzz 
import mysql.connector
import pandas as pd
import os

params_dict = {
    "host": "localhost",
    "port": 3306,
    "username": "root",
    "password": "",
    "sourceTable": "dbo.Usuarios",
    "destTable": "crm.Clientes",
    "src_dest_mappings": {
        "first_name": "nombre",
        "last_name": "apellido"
    }
}

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

    best_matches = []

    # 🔹 Solo el mejor resultado por scorer
    for scorer in scorers:
        match = process.extractOne(
            query=processed_query,
            choices=[item['query_match'] for item in choices_data],
            scorer=scorer,
            score_cutoff=score_cutoff,
            processor=processor
        )

        if match:
            match_value, score, index = match
            matched_item = choices_data[index]
            best_matches.append({
                'Scorer': scorer.__name__,
                'Query': queryRecord,
                'Nombre': matched_item['match_record_values'].get('nombre'),
                'Apellido': matched_item['match_record_values'].get('apellido'),
                'Score': score
            })

    return pd.DataFrame(best_matches)


# -------------------------
# 🧪 Prueba local con datos falsos
# -------------------------
if __name__ == "__main__":
    choices = [
        {"DestRecordId": 1, "nombre": "Juan", "apellido": "Perz"},
        {"DestRecordId": 2, "nombre": "Juana", "apellido": "Perez"},
        {"DestRecordId": 3, "nombre": "John", "apellido": "Smith"},
        {"DestRecordId": 4, "nombre": "Pedro", "apellido": "García"}
    ]

    query = "Juan Perez"
    df_resultados = fuzzy_match(query, choices)

    print(f"🔍 Buscando: {query}\n")
    print(df_resultados)

    # 📌 Si quieres exportar a CSV o Excel:
    # df_resultados.to_csv("resultados.csv", index=False)
    # df_resultados.to_excel("resultados.xlsx", index=False)
