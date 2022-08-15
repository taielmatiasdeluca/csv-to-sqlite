from datetime import datetime
import pathlib
import sys
import os
import sqlite3 as sql

# Autor: Taiel De Luca @ 2022

#
#   Como usar el script-> Se deberan ingrasar los csv en la carpeta dedidaca llamada csv luego ejecutar el script con python
#   generara un archivo llamado database.db que tendra almacenadas las tablas con los valores en el csv, tambien se creara
#   un archivo de log en el cual se podran ver informacion de la base de datos.
#


#Variables
separadores = [',','|',';']
prohibitedCaracters = [",",'"',"'",' ']




#Devuelve el nombre y ubicacion de el csv
def getNameAndPath(argv):
    cantParametros = len(argv)
    pathCsv = sys.argv[1]
    nameDb = 'dataBase.db'
    if cantParametros == 3:
        nameDb = sys.argv[2]
        if pathlib.Path(nameDb).suffix != '.db':
            nameDb += '.db'
    return pathlib.Path(pathCsv),nameDb

#Devuelve el tipo de separador del csv
def getCsvSeparator(csvPath):
    csv = open(csvPath,'r')
    csvInfo = list
    while True:
        line = csv.readline()
        if not line:
            break
        for caracter in line:
            for separador in separadores:
                if caracter == separador:
                    return separador
   
#Crea la tabla si no existe
def createDB(dbName,tableName,colName,colType): 
    coneccion = sql.connect(f'./{dbName}')
    cursor = coneccion.cursor()
    sentencia = f"""CREATE TABLE IF NOT EXISTS '{tableName}' (
            idDB INTEGER PRIMARY KEY AUTOINCREMENT """
    for i in range(len(colName)):
        colName[i] = colName[i].strip()
        if(colName[i] == ''):
            colName[i] = 'None'
        try:
            int(colName[i])
            colName[i] = f'columna{i}'
        except:
            pass
        sentencia += f', "{colName[i]}" {colType[i]} '
    sentencia += ');'
    print(f'->  Se ha creado la tabla {tableName} en la base de datos {dbName}  <-')
    print(sentencia)
    cursor.execute(sentencia)
    coneccion.commit()
    return coneccion,cursor

#Devuelve todo el csv en una lista ordenada
def getAllCsv(csvPath):
    csv = open(csvPath,'r')
    csvSep = getCsvSeparator(csvPath)
    csvInfo = list()
    while True:
        line = csv.readline()
        if not line:
            break
        line = line.replace('\n','')
        info = line.split(sep=csvSep)
        csvInfo.append(info)     
    return csvInfo

#Devuelve el tipo de columnas
def getColumnasType(infoCol):
    columnas = list()
    for i in range(len(infoCol)):
        try:
            info = infoCol[i]
            info = info.strip()
            info = info.replace('-','/')
            datetime.strptime(info,'%Y/%m/%d %H:%M:%S')
            columnas.append(f'DATETIME')
        except:
            info = cleanValue(infoCol[i])
            try:
                int(info)
                columnas.append(f'INTEGER')
                continue
            except:
                try:
                    float(info)
                    columnas.append(f'REAL')
                    continue
                except:
                    columnas.append(f'TEXT')
                    continue
    return columnas      

#Limpia el valor a ingresar teniendo en cuenta los valores en la lista prohibitedCaracters
def cleanValue(value):
    newValue = ""
    for letter in value:
        safe = True
        for caracter in prohibitedCaracters:
            if letter == caracter:
                safe = False
        if safe :
            newValue += letter
    return newValue

#Copia la info del csv en la base de datos
def copyCsvIntoDb(csvInfo,colName,db,cursor,tableName):
    colType = getColumnasType(csvInfo[1])
    for item in csvInfo:
        if item is csvInfo[0]:
            continue
        sentencia = f""" INSERT INTO '{tableName}' values(null"""
        for i in range(len(colName)):
            info = cleanValue(item[i])

            if colType[i] == f'REAL':
                try:
                    float(info)
                except:
                    info = 0
                sentencia += f",{info}"
            elif colType[i] == f'INTEGER':
                try:
                    int(info)
                except:
                    info = 0
                sentencia += f",{info}"
            else:
                sentencia += f",'{info}'"

        sentencia += ');'
        print(f'Ingresando -> {sentencia}')
        
        cursor.execute(sentencia)
        db.commit()
    return True

#Crea el log
def CreateLog(columnas,columnasType):
    info = '----->INFORMACION DE LA BASE DE DATOS CREADA<-----\nLas Columnas creadas en la base de datos son las siguientes con los siguientes tipo de datos\n\n\n  Nombre ->  idDB   Tipo ->  AutoIncremental\n'
    for i in range(len(columnas)):
        info += f'  Nombre ->  {columnas[i]} Tipo ->  {columnasType[i]} \n'
    file = open(f'.log.txt','a+')
    file.write(info)
    file.close()
    return True


def main():
    #Por cada archivo
    for file in os.listdir('./csv'):
        path = pathlib.Path(file)#Consigue el nombre del archivo

        nombreTabla = path.stem

        csvContent = getAllCsv(f'./csv/{file}')#Lee todo el contenido de csv

        #Si el archivo esta vacio
        try:
            check = csvContent[1]
        except:
            #Pasa al siguiente
            continue

        colType =  getColumnasType(csvContent[1])   #Tipos de valores basado en la segunda fila del csv
        colNames = csvContent[0]    #Nombres de las columnas segun el la primer fila 
        db,cursor = createDB('database.db',nombreTabla,csvContent[0],colType)   #Cursor y conexion con la base de datos
        copyCsvIntoDb(csvContent,colNames,db,cursor,nombreTabla)    #copia el csv en la base de datos
        CreateLog(colNames,colType)     #Crea el log con infomacion de la base de datos creada


if __name__ == '__main__':
    main()