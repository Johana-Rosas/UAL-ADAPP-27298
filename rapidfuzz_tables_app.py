from modulos import execute_dynamic_matching

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

if __name__ == "__main__":
    resultados = execute_dynamic_matching(params_dict, score_cutoff=70)
    print(resultados)
