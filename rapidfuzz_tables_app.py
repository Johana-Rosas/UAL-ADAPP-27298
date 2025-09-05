from modulos import (
    execute_dynamic_matching,
    display_results,
    export_results_to_excel,
    export_results_to_csv,
    mostrar_columnas_disponibles,
    preparar_resultados,
    separar_matched_unmatched,
    importar_archivo,
    insertar_en_tabla_mysql
)

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
#importar archivo 
if __name__ == "__main__":
    print("¿Deseas importar un archivo para insertarlo en la base de datos?")
    print("1 - Sí")
    print("2 - No (continuar con matching)")
    opcion_importar = input("Selecciona una opción (1/2): ").strip()

    if opcion_importar == "1":
        filepath = input("Ruta del archivo a importar (.csv o .xlsx): ").strip()
        datos_importados = importar_archivo(filepath)
        print(f"Archivo importado con {len(datos_importados)} registros.")
        db = input("Nombre de la base de datos destino: ").strip()
        tabla = input("Nombre de la nueva tabla destino: ").strip()
        insertar_en_tabla_mysql(
            host=params_dict["host"],
            username=params_dict["username"],
            password=params_dict["password"],
            port=params_dict["port"],
            database=db,
            table_name=tabla,
            data=datos_importados
        )
        print("Importación finalizada. Puedes ejecutar el matching ahora si lo deseas.")
        exit()

    #hasta aqui

    resultados = execute_dynamic_matching(params_dict, score_cutoff=70)

    # Mostrar columnas disponibles
    columnas_disponibles = mostrar_columnas_disponibles(resultados)

    # Preguntar columnas a mostrar/exportar
    columnas_input = input("Ingrese los nombres de las columnas a mostrar/exportar (separados por coma, vacío para todas): ").strip()
    columnas_seleccionadas = [col.strip() for col in columnas_input.split(",") if col.strip()] if columnas_input else []

    # Pedir nuevos nombres para las columnas seleccionadas y para 'score' y 'full_name'
    nuevos_nombres = {}
    columnas_para_renombrar = set(columnas_seleccionadas)
    if 'score' in columnas_disponibles:
        columnas_para_renombrar.add('score')
    columnas_para_renombrar.add('full_name')
    for col in columnas_para_renombrar:
        nuevo_nombre = input(f"Ingrese el nuevo nombre para la columna '{col}' (dejar vacío para mantener): ").strip()
        if nuevo_nombre:
            nuevos_nombres[col] = nuevo_nombre

    # Preparar resultados (score como porcentaje y full_name)
    resultados_filtrados = preparar_resultados(resultados, columnas_seleccionadas, nuevos_nombres)

    # Mostrar resultados filtrados
    display_results(resultados_filtrados, as_dataframe=True)

    # Preguntar cómo exportar
    opcion = input("¿Deseas exportar los resultados a (1) Excel o (2) CSV?: ").strip()

    # Validaciones de nombre de archivo y límite de filas para ambos tipos
    if opcion == "1":
        filename = input(" Ingresa el nombre del archivo de salida (.xlsx): ").strip()
        if not filename.endswith(".xlsx"):
            filename += ".xlsx"
    elif opcion == "2":
        filename = input(" Ingresa el nombre del archivo de salida (.csv): ").strip()
        if not filename.endswith(".csv"):
            filename += ".csv"
    else:
        print(" Opción inválida. No se exportaron los resultados.")
        exit()

    try:
        max_rows = int(input(" Ingresa el número máximo de filas a exportar (0 para cancelar): ").strip())
        if max_rows == 0:
            print(" Exportación cancelada. El archivo no fue creado.")
            exit()
    except ValueError:
        print(" Valor inválido. Se exportarán todas las filas.")
        max_rows = None

    # Limitar filas si es necesario
    if max_rows and max_rows > 0:
        resultados_filtrados = resultados_filtrados[:max_rows]

    # Exportar según el tipo de archivo
    if opcion == "1":
        export_results_to_excel(resultados_filtrados, filename)
    elif opcion == "2":
        export_results_to_csv(resultados_filtrados, filename)

    # Separar registros según el score
    matched, unmatched = separar_matched_unmatched(resultados_filtrados)

    # Preguntar qué grupo exportar
    print("\n¿Qué registros deseas exportar?")
    print("1 - Solo matched (score >= 97%)")
    print("2 - Solo unmatched (score < 97%)")
    print("3 - Ambos grupos en archivos separados")
    grupo = input("Selecciona una opción (1/2/3): ").strip()

    # Exportar según la selección
    if grupo == "1":
        export_data = matched
        export_label = "matched"
        filename = input(f" Ingresa el nombre del archivo de salida para {export_label} ({'.xlsx' if opcion == '1' else '.csv'}): ").strip()
        if opcion == "1" and not filename.endswith(".xlsx"):
            filename += ".xlsx"
        elif opcion == "2" and not filename.endswith(".csv"):
            filename += ".csv"
        if opcion == "1":
            export_results_to_excel(export_data, filename)
        elif opcion == "2":
            export_results_to_csv(export_data, filename)
    elif grupo == "2":
        export_data = unmatched
        export_label = "unmatched"
        filename = input(f" Ingresa el nombre del archivo de salida para {export_label} ({'.xlsx' if opcion == '1' else '.csv'}): ").strip()
        if opcion == "1" and not filename.endswith(".xlsx"):
            filename += ".xlsx"
        elif opcion == "2" and not filename.endswith(".csv"):
            filename += ".csv"
        if opcion == "1":
            export_results_to_excel(export_data, filename)
        elif opcion == "2":
            export_results_to_csv(export_data, filename)
    elif grupo == "3":
        filename_matched = input(f" Ingresa el nombre del archivo de salida para matched ({'.xlsx' if opcion == '1' else '.csv'}): ").strip()
        filename_unmatched = input(f" Ingresa el nombre del archivo de salida para unmatched ({'.xlsx' if opcion == '1' else '.csv'}): ").strip()
        if opcion == "1":
            if not filename_matched.endswith(".xlsx"):
                filename_matched += ".xlsx"
            if not filename_unmatched.endswith(".xlsx"):
                filename_unmatched += ".xlsx"
            export_results_to_excel(matched, filename_matched)
            export_results_to_excel(unmatched, filename_unmatched)
        elif opcion == "2":
            if not filename_matched.endswith(".csv"):
                filename_matched += ".csv"
            if not filename_unmatched.endswith(".csv"):
                filename_unmatched += ".csv"
            export_results_to_csv(matched, filename_matched)
            export_results_to_csv(unmatched, filename_unmatched)
        print(" Archivos exportados para ambos grupos.")
    else:
        print(" Opción inválida. No se exportaron los resultados.")
        exit()