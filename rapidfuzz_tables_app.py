from modulos import execute_dynamic_matching, display_results, export_results_to_excel, export_results_to_csv

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

    # Mostrar resultados
    display_results(resultados, as_dataframe=True)

    # 📂 Preguntar cómo exportar
    opcion = input("¿Deseas exportar los resultados a (1) Excel o (2) CSV?: ").strip()

    if opcion == "1":
        export_results_to_excel(resultados)
    elif opcion == "2":
        export_results_to_csv(resultados)
    else:
        print("⚠️ Opción inválida. No se exportaron los resultados.")
