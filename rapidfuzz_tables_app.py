from modulos import execute_dynamic_matching, display_results, export_results_to_csv

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


    choice = input("¿Ver resultados como DataFrame (D) o como Diccionario (C)? ").strip().lower()
    if choice == "d":
        display_results(resultados, as_dataframe=True)
    else:
        display_results(resultados, as_dataframe=False)

    # Preguntar si se desea exportar
    export_choice = input("¿Quieres exportar los resultados a CSV? (S/N): ").strip().lower()
    if export_choice == "s":
        filename = input("Ingresa el nombre del archivo (ejemplo: salida.csv): ").strip()
        if filename == "":
            filename = "resultados.csv"
        export_results_to_csv(resultados, filename)
