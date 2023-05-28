# -*- coding: utf-8 -*-
"""
Created on Thu May 25 07:32:50 2023

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
    #hoy = date(2023,5,26)
    if hoy.weekday() >= 0 and hoy.weekday() < 5:
        with open("./config.json","r") as file:
            config = json.load(file)
        
        BASE_URL_API = config['DATA_API']['URL_BASE']
        BASE_CURRENCY = config['DATA_API']['FROM_CURRENCY']
        
        BASE_BD_HOST = config['DATA_BD']['BD_HOST']
        BASE_BD_NAME = config['DATA_BD']['BD_NAME']
        BASE_BD_USER = config['DATA_BD']['BD_USER']
        BASE_BD_PASS = config['DATA_BD']['BD_PASS']
        BASE_BD_PORT = config['DATA_BD']['BD_PORT']
        
        url_request = f'{BASE_URL_API}/{hoy}?from={BASE_CURRENCY}'
        request_currencies = None
        data_currencies = None
        request_currencies = requests.get(url_request)
        if request_currencies.status_code == 200:
            data_currencies = request_currencies.json()
        else:
            print("Peticion incorrecta")
        
        
        if data_currencies != None:
            frame_currencies = pnd.DataFrame(data_currencies)
            frame_currencies.insert(0,'currency',frame_currencies.index)
            
            conexion = conexion_redshift(HOST = BASE_BD_HOST, PORT = BASE_BD_PORT, NAME = BASE_BD_NAME, USER = BASE_BD_USER, PASSWORD = BASE_BD_PASS)
            TABLE_NAME = config['DATA_BD']['DATA_TABLE']['NAME_TABLE']
            TABLE_COLUMNS = config['DATA_BD']['DATA_TABLE']['COLUMNS']
            
            carga_hoy = carga_red.getLoadToday(conexion.Conexion, TABLE_NAME, TABLE_COLUMNS, operation_date = hoy) 
            if carga_hoy == None:
                carga_red.create_table_exchange(conexion.Conexion, TABLE_NAME)
                carga_red.load_actual_exchange(conexion.Conexion, TABLE_NAME, TABLE_COLUMNS, frame_currencies)    
            else:
                print("La carga del dia de hoy ya fue realizada")
            
            
    else:
        print('El dia de hoy no se realiza la carga')
except Exception as error:
    print('Surgio el siguiente error al realizar el proceso de extraccion: ')
    print(error)

print('================EL PROCESO HA CONCLUIDO================')