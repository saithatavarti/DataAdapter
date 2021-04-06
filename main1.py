from fastapi import FastAPI
import pyodbc
import sqlalchemy as sal
from sqlalchemy import create_engine
import pandas as pd


app = FastAPI()

@app.get('/')
def index():
    return {'data':{'name':'krishna'}}

# @app.get('/about')
# def index():
#     return {'data':"about page"}

# @app.get('/about/{id}')
# def abt (id):
#     return {'data':{id}} 


# @app.get("/items")
# def test():
# 	with open(r'test.yaml') as file:
# 		documents = yaml.safe_load(file)
# 	adapter = str(documents["parameters"]["adapter"])    
# 	username = str(documents["parameters"]["username"])
# 	password = str(documents["parameters"]["password"])
# 	query = str(documents["query"])
# 	db = documents["parameters"]["database"]
# 	port = str(documents["parameters"]["database_port"])
# 	link =""
# 	link = adapter+"://"+username+":"+password+"@localhost:"+port+"/"+db
# 	# db connection 
# 	engine = create_engine(link,echo=False)
# 	connection=engine.connect()
# 	lst = connection.execute(query).fetchall()
# 	return(lst)

@app.get("/items")
def test():
    engine = sal.create_engine('mssql+pyodbc://LAPTOP-CMNLMDRF\SQLEXPRESS/students?driver=SQL Server?Trusted_Connection=yes')
    conn = engine.connect()
    result = engine.execute("select * from details").fetchall()
    return(result)
    
    