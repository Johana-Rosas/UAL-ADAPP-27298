 la funcion
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
score_cutoff=0 - es el puntaje minimo para aceptar un mach



def execute_dynamic_matching(params_dict, score_cutoff=0):

esta funcion hace la comparacion completa entre una tabla de origen y una destino 

params_dict - guarda todos los parametros de configuracion como el server, database, usernam, pasword

score_cutoff=0 - es el puntaje minimo de comparacion entre tablas para machear.