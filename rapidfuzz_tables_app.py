from modulos import (
    execute_dynamic_matching,
    display_results,
    export_results_to_excel,
    export_results_to_csv,
    mostrar_columnas_disponibles,
    preparar_resultados
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

if __name__ == "__main__":
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
        filename = input("📂 Ingresa el nombre del archivo de salida (.xlsx): ").strip()
        if not filename.endswith(".xlsx"):
            filename += ".xlsx"
    elif opcion == "2":
        filename = input("📂 Ingresa el nombre del archivo de salida (.csv): ").strip()
        if not filename.endswith(".csv"):
            filename += ".csv"
    else:
        print("⚠️ Opción inválida. No se exportaron los resultados.")
        exit()

    try:
        max_rows = int(input("🔢 Ingresa el número máximo de filas a exportar (0 para cancelar): ").strip())
        if max_rows == 0:
            print("⚠️ Exportación cancelada. El archivo no fue creado.")
            exit()
    except ValueError:
        print("⚠️ Valor inválido. Se exportarán todas las filas.")
        max_rows = None

    # Limitar filas si es necesario
    if max_rows and max_rows > 0:
        resultados_filtrados = resultados_filtrados[:max_rows]

    # Exportar según el tipo de archivo
    if opcion == "1":
        export_results_to_excel(resultados_filtrados, filename)
    elif opcion == "2":
        export_results_to_csv(resultados_filtrados, filename)
