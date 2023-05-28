# -*- coding: utf-8 -*-
"""
Created on Thu May 25 07:34:40 2023
Clase con los metodos necesarios para las operaciones de redshift
@author: danie
"""

from psycopg2.extras import execute_values

#Metodo que crea la tabla para la carga del tipo de cambio, si es que esta no existe
def create_table_exchange(conect,table_name):
    try:
        sql_table = f"""create table if not exists {table_name} (
        currency varchar(3),
	    operation_date date,
	    ammount float,
	    base_currency varchar(3),
	    rates float,
	    primary key(currency,operation_date),
        unique(currency,operation_date)
        );"""
        cur = conect.cursor()
        cur.execute(sql_table)
        #cur.execute("COMMIT")
        conect.commit()
        print(f"""Se ha creado la tabla {table_name} exitosamente""")
    except Exception as error:
        print('Ocurrio el siguiente error al crear la tabla de carga')
        print(error)

#Metodo que realiza la carga de la informacion del tipo de cambio obtenido de la API
def load_actual_exchange(conexion,table_name,columns,data_f):
    try:
        cols = ','.join(columns)
        insert_sql = f"insert into {table_name} ({cols}) values %s"
        
        values = [tuple(x) for x in data_f.to_numpy()]
        cur = conexion.cursor()
        cur.execute('BEGIN')
        execute_values(cur,insert_sql,values)
        #cur.execute('COMMIT')
        conexion.commit()
        print('Se inserto la informacion exitosamente')
    except Exception as error:
        print('Ocurrio el siguiente error al insertar los datos')
        print(error)
      
#Metodo que consulta la informacion de determinada fecha en la BD y devuelve el resultado
def getLoadToday(conexion,table_name,columns,operation_date):
    try:
        cols = ','.join(columns)
        select_sql = f"select {cols} from {table_name} where operation_date = '{operation_date}'"
        cursor = conexion.cursor()
        cursor.execute(select_sql)
        result = cursor.fetchone()
        return result
    except Exception as error:
        print("Ocurrio el siguiente error al consultar la tabla")
        print(error)