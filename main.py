#Importing required libraries
from fastapi import FastAPI, Request
from sqlalchemy import create_engine
import yaml, json
import pandas as pd
import cx_Oracle

app = FastAPI()

@app.get("/")
def start():
    return{"Message" : "Please enter a YAML filename in URL to return JSON data"}

#Opening connection to database and returning data in JSON format
@app.get("/{file_name}")
def read_file(file_name, request: Request):
    my_dict = {"file" : file_name}
    my_value = (my_dict["file"])
    try:
        with open(my_value) as req:
            document = yaml.safe_load(req)
            db_type = str(document["development"]["db_type"])

            #Returning JSON data if database is SQLite
            if(db_type == "sqlite"):

                db_name = str(document["development"]["database"])
                query = str(document["query"])
                link = "sqlite:///"+db_name+".db"
                engine = create_engine(link)
                connection = engine.connect()
                result = connection.execute(query).fetchall()
                df = pd.read_json (r'D:\Projects\Data Adapters\sqlite_data_adapter\response_1617612890440.json')
                df.to_csv (r'D:\Projects\Data Adapters\sqlite_data_adapter\myfile.csv', index = None)
                return (result)

            #Returning JSON data if database is MariaDB
            elif(db_type == "mariadb"):

                adapter=str(document["development"]["adapter"])    
                username=str(document["development"]["username"])
                password=str(document["development"]["password"])
                query=str(document["query"])
                dbs=document["development"]["database"]
                engine = create_engine(adapter+"://"+username+":"+password+"@localhost:3306/"+dbs+"?charset=utf8mb4")
                connection=engine.connect()
                result = connection.execute(query).fetchall()
                return(result)

            #Returning JSON data if database is PostgreSQL
            elif(db_type == "postgresql"):

                adapter = str(document["development"]["adapter"])    
                username = str(document["development"]["username"])
                password = str(document["development"]["password"])
                query=str(document["query"])
                db = document["development"]["database"]
                port = str(document["development"]["database_port"])
                link = adapter+"://"+username+":"+password+"@localhost:"+port+"/"+db
                engine = create_engine(link,echo=False)
                connection=engine.connect()
                result = connection.execute(query).fetchall()
                return(result)

            #Returning JSON data if database is Oracle
            elif(db_type == "oracle"):
                adapter=str(document["development"]["adapter"])    
                username=str(document["development"]["username"])
                password=str(document["development"]["password"])
                query=str(document["query"])
                dbs=document["development"]["database"]
                link=adapter+"://"+username+":"+password+"@localhost:1521/"+dbs
                engine = create_engine(link)
                connection=engine.connect()
                result = connection.execute(query).fetchall()
                return(result)

            #Returning JSON data if database is MySQL
            elif(db_type == "mysql"):
                adapter=str(document["development"]["adapter"])
                username=str(document["development"]["username"])
                password=str(document["development"]["password"])
                query=str(document["query"])
                dbs=document["development"]["database"]
                link=adapter+"://"+username+":"+password+"@localhost/"+dbs
                engine = create_engine(link)
                connection=engine.connect()
                result = connection.execute(query).fetchall()
                return(result)
    except:
        return{"Message" : "Please specify correct filename in URL"}