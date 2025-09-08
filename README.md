 La funcion
 def connect_to_azure_sql(server, database, username, password):
 es la que hace la conexion a una base de datos de azure, los parametros puestos en la funcion son para traer lo que es 
 server - el nombre del servidor que va a conectar
 database - la base de datos con la que se va a conectar 
 username- es el nombre del usuario
 password - la contraseña del usuario 


def fuzzy_match(queryRecord, choices, score_cutoff=0):

esta funcion es la que se encarga de comparar las bd de clientes y usuarios para comparar sus valores y si dichos valores son iguales, los almacena en choices_data[]
queryRecord - es el valor que se desea buscar o machear
choices - lista de destino 
score_cutoff=0 - es el puntaje minimo para aceptar un match



def execute_dynamic_matching(params_dict, score_cutoff=0):

esta funcion hace la comparacion completa entre una tabla de origen y una destino 

params_dict - guarda todos los parametros de configuracion como el server, database, usernam, pasword

score_cutoff=0 - es el puntaje minimo de comparacion entre tablas para machear.

*** Cambios y optimizaciones realizadas en insertMysql.py:

1.Función genérica de inserción.
En lugar de tener funciones separadas para la tabla Usuarios y la tabla Clientes, ahra se usa una función insert_from_csv que recibe:
-Conexión a la BD
-Nombre de la tabla
-Columnas a insertar
-Columnas a actualizar en caso de duplicados
-Archivo CSV de origen


2.Inserción en lote con executemany
En vez de ejecutar INSERT por cada fila, ahora se cargan todas las filas en memoria y se insertan/actualizan en una sola llamada optimizada a MySQL, lo cual mejora el rendimiento.


*** Cambios realizados en rapidfuzz_tables_app.py:


Devuelve una lista de todos los matches que superan el score_cutoff, permitiendo obtener múltiples coincidencias por registro.

Se cambió process.extractOne a process.extract de rapidfuzz.

Se hace una copia del diccionario de datos de la fila (dict_query_records_copy) antes de agregar cada match.
Esto previene que múltiples matches sobrescriban datos del mismo registro.

Ajuste de parámetros de ejemplo:
El score_cutoff predeterminado se cambió de 80 a 70.



ENTREGABLE SEMANAL #2
La funcion  importar_archivo_y_insertar_tabla hace:
Importa un archivo CSV o Excel y lo inserta en la tabla 'matched_record'
 usando el stored procedure sp_insert_file_matched_record_27298.
Sobrescribe la tabla si ya existe.

 Convención de nombres
El stored procedure se llama:
sp_insert_file_matched_record_27298
cumpliendo la convención:
sp_[process]_[source]_[target]_[27298]
donde:
process: insert_file
source: (el archivo)
target: matched_record
27298: tu identificador