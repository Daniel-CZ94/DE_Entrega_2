# -*- coding: utf-8 -*-
"""
Created on Thu May 25 07:32:50 2023
Clase principal que realiza las operaciones de carga, descarga, validacion y creacion
@author: danie
"""

import json
import pandas as pnd
import requests
from BD.conexion_redshift import conexion_redshift
import BD.load_redshift as carga_red
from datetime import date

BASE_URL_API = None
BASE_CURRENCY = None

BASE_BD_HOST = None
BASE_BD_NAME = None
BASE_BD_USER = None
BASE_BD_PASS = None
BASE_BD_PORT = None


try:
    print("================EL PROCESO HA INICIADO================")
    hoy = date.today()
    #Validamos que el dia actual no corresponda al fin de semana, ya que
    #los fines de semana no se generan los tipos de cambio y puede ocasionar
    #que los datos se dupliquen
    if hoy.weekday() >= 0 and hoy.weekday() < 5:
        with open("./config.json","r") as file:
            config = json.load(file)
        
        #Obtenemos los datos correspondientes a la base de datos y a la API de consulta,
        #del archivo config.json
        BASE_URL_API = config['DATA_API']['URL_BASE']
        BASE_CURRENCY = config['DATA_API']['FROM_CURRENCY']
        
        BASE_BD_HOST = config['DATA_BD']['BD_HOST']
        BASE_BD_NAME = config['DATA_BD']['BD_NAME']
        BASE_BD_USER = config['DATA_BD']['BD_USER']
        BASE_BD_PASS = config['DATA_BD']['BD_PASS']
        BASE_BD_PORT = config['DATA_BD']['BD_PORT']
        
        TABLE_NAME = config['DATA_BD']['DATA_TABLE']['NAME_TABLE']
        TABLE_COLUMNS = config['DATA_BD']['DATA_TABLE']['COLUMNS']
        #Creamos la conexion a la BD
        conexion = conexion_redshift(HOST = BASE_BD_HOST, PORT = BASE_BD_PORT, NAME = BASE_BD_NAME, USER = BASE_BD_USER, PASSWORD = BASE_BD_PASS)
        #Consultamos en la BD si existe informacion cargada del dia actual
        carga_hoy = carga_red.getLoadToday(conexion.Conexion, TABLE_NAME, TABLE_COLUMNS, operation_date = hoy) 
        #Si existe informacion cargada del dia actual, el proceso concluye, en caso contrario
        #procede a realizar la carga
        if carga_hoy == None:
            #Generamos la URL de la API
            url_request = f'{BASE_URL_API}/{hoy}?from={BASE_CURRENCY}'
            request_currencies = None
            data_currencies = None
            #Realizamos la peticion a la API
            request_currencies = requests.get(url_request)
            if request_currencies.status_code == 200:
                data_currencies = request_currencies.json()
            else:
                print("Peticion incorrecta")
            
            #Validamos que exista informacion obtenida de la API, en
            #caso contrario, el proceso concluye
            if data_currencies != None:
                #Insertamos la informacion leida en un dataframe
                frame_currencies = pnd.DataFrame(data_currencies)
                #Insertamos en el dataframe la columna "Currency", la cual vamos a requerir en la BD
                frame_currencies.insert(0,'currency',frame_currencies.index)
                #Creamos la tabla en Redshift (si es que esta no existe)
                carga_red.create_table_exchange(conexion.Conexion, TABLE_NAME)
                #Cargamos la informacion obtenida de la API en la BD
                carga_red.load_actual_exchange(conexion.Conexion, TABLE_NAME, TABLE_COLUMNS, frame_currencies)    
        else:
            print("La carga del dia de hoy ya fue realizada")
    else:
        print('El dia de hoy no se realiza la carga')
except Exception as error:
    print('Surgio el siguiente error al realizar el proceso de extraccion: ')
    print(error)

print('================EL PROCESO HA CONCLUIDO================')