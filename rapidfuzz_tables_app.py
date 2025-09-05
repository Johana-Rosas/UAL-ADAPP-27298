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
        try:
            datos_importados = importar_archivo(filepath)
            if not datos_importados:
                print("❌ No se pudo importar el archivo o está vacío.")
                exit()
        except Exception as e:
            print(f"❌ Error al importar el archivo: {e}")
            exit()
        print(f"Archivo importado con {len(datos_importados)} registros.")
        db = input("Nombre de la base de datos destino: ").strip()
        tabla = input("Nombre de la nueva tabla destino: ").strip()
        try:
            insertar_en_tabla_mysql(
                host=params_dict["host"],
                username=params_dict["username"],
                password=params_dict["password"],
                port=params_dict["port"],
                database=db,
                table_name=tabla,
                data=datos_importados
            )
            print("✅ Importación finalizada. Puedes ejecutar el matching ahora si lo deseas.")
        except Exception as e:
            print(f"❌ Error al insertar en la base de datos: {e}")
        exit()

    # Matching y exportación
    try:
        resultados = execute_dynamic_matching(params_dict, score_cutoff=70)
    except Exception as e:
        print(f"❌ Error durante el matching: {e}")
        exit()

    columnas_disponibles = mostrar_columnas_disponibles(resultados)
    columnas_input = input("Ingrese los nombres de las columnas a mostrar/exportar (separados por coma, vacío para todas): ").strip()
    columnas_seleccionadas = [col.strip() for col in columnas_input.split(",") if col.strip()] if columnas_input else []

    nuevos_nombres = {}
    columnas_para_renombrar = set(columnas_seleccionadas)
    if 'score' in columnas_disponibles:
        columnas_para_renombrar.add('score')
    columnas_para_renombrar.add('full_name')
    for col in columnas_para_renombrar:
        nuevo_nombre = input(f"Ingrese el nuevo nombre para la columna '{col}' (dejar vacío para mantener): ").strip()
        if nuevo_nombre:
            nuevos_nombres[col] = nuevo_nombre

    try:
        resultados_filtrados = preparar_resultados(resultados, columnas_seleccionadas, nuevos_nombres)
    except Exception as e:
        print(f"❌ Error al preparar los resultados: {e}")
        exit()

    try:
        display_results(resultados_filtrados, as_dataframe=True)
    except Exception as e:
        print(f"❌ Error al mostrar los resultados: {e}")

    opcion = input("¿Deseas exportar los resultados a (1) Excel o (2) CSV?: ").strip()

    if opcion == "1":
        filename = input(" Ingresa el nombre del archivo de salida (.xlsx): ").strip()
        if not filename.endswith(".xlsx"):
            filename += ".xlsx"
    elif opcion == "2":
        filename = input(" Ingresa el nombre del archivo de salida (.csv): ").strip()
        if not filename.endswith(".csv"):
            filename += ".csv"
    else:
        print("❌ Opción inválida. No se exportaron los resultados.")
        exit()

    try:
        max_rows = int(input(" Ingresa el número máximo de filas a exportar (0 para cancelar): ").strip())
        if max_rows == 0:
            print("⚠️ Exportación cancelada. El archivo no fue creado.")
            exit()
    except ValueError:
        print("⚠️ Valor inválido. Se exportarán todas las filas.")
        max_rows = None

    if max_rows and max_rows > 0:
        resultados_filtrados = resultados_filtrados[:max_rows]

    try:
        if opcion == "1":
            export_results_to_excel(resultados_filtrados, filename)
        elif opcion == "2":
            export_results_to_csv(resultados_filtrados, filename)
    except Exception as e:
        print(f"❌ Error al exportar el archivo: {e}")

    # Separar registros según el score
    try:
        matched, unmatched = separar_matched_unmatched(resultados_filtrados)
    except Exception as e:
        print(f"❌ Error al separar matched/unmatched: {e}")
        matched, unmatched = [], []

    print("\n¿Qué registros deseas exportar?")
    print("1 - Solo matched (score >= 97%)")
    print("2 - Solo unmatched (score < 97%)")
    print("3 - Ambos grupos en archivos separados")
    grupo = input("Selecciona una opción (1/2/3): ").strip()

    try:
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
            print("✅ Archivos exportados para ambos grupos.")
        else:
            print("❌ Opción inválida. No se exportaron los resultados.")
    except Exception as e:
        print(f"❌ Error al exportar archivos matched/unmatched: {e}")

    # --- Importación final al terminar todo ---
    print("\n¿Deseas importar un archivo para insertarlo en la base de datos?")
    print("1 - Sí")
    print("2 - No (salir)")
    opcion_importar_final = input("Selecciona una opción (1/2): ").strip()

    if opcion_importar_final == "1":
        filepath = input("Ruta del archivo a importar (.csv o .xlsx): ").strip()
        try:
            datos_importados = importar_archivo(filepath)
            if not datos_importados:
                print("❌ No se pudo importar el archivo o está vacío.")
            else:
                print(f"Archivo importado con {len(datos_importados)} registros.")
                columnas_disponibles = list(datos_importados[0].keys())
                print("\nColumnas encontradas en el archivo:")
                print(", ".join(columnas_disponibles))
                nuevos_nombres = {}
                for col in columnas_disponibles:
                    nuevo_nombre = input(f"Ingrese el nuevo nombre para la columna '{col}' (dejar vacío para mantener): ").strip()
                    if nuevo_nombre:
                        nuevos_nombres[col] = nuevo_nombre
                if nuevos_nombres:
                    datos_importados = [
                        {nuevos_nombres.get(k, k): v for k, v in fila.items()}
                        for fila in datos_importados
                    ]
                db = input("Nombre de la base de datos destino: ").strip()
                tabla = input("Nombre de la nueva tabla destino: ").strip()
                try:
                    insertar_en_tabla_mysql(
                        host=params_dict["host"],
                        username=params_dict["username"],
                        password=params_dict["password"],
                        port=params_dict["port"],
                        database=db,
                        table_name=tabla,
                        data=datos_importados
                    )
                    print(f"✅ Datos importados e insertados en la tabla '{tabla}' de la base '{db}'.")
                except Exception as e:
                    print(f"❌ Error al insertar en la base de datos: {e}")
        except Exception as e:
            print(f"❌ Error al importar el archivo: {e}")
    else:
        print("Fin del programa.")