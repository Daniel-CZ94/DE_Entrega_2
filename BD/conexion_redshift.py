# -*- coding: utf-8 -*-
"""
Created on Thu May 25 07:33:15 2023

@author: danie
"""

import psycopg2

class conexion_redshift():
    HOST = None
    PORT = None
    NAME = None
    USER = None
    PASSWORD = None

    Conexion = None
    
    def __init__(self,HOST, PORT,NAME,USER,PASSWORD):
        self.HOST = HOST
        self.PORT = PORT
        self.NAME = NAME
        self.USER = USER
        self.PASSWORD = PASSWORD

        try:
            self.Conexion = psycopg2.connect(host = self.HOST,database = self.NAME,user = self.USER,password = self.PASSWORD,port = self.PORT)
            print('Conexion a la BD exitosa')
        except Exception as e:
            print('Ocurrio un error al conectarse a la BD')
            print(e)