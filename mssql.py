from fastapi import FastAPI
import pyodbc
import sqlalchemy as sal
from sqlalchemy import create_engine
import pandas as pd
import yaml


app = FastAPI()

@app.get('/')
def index():
    return {'data':{'name':'krishna'}}


@app.get("/items/{choose_id}")
def test(choose_id):
    with open(choose_id+".yaml") as file:
        documents = yaml.safe_load(file)
    adapter = str(documents["parameters"]["adapter"])
    username = str(documents["parameters"]["username"])
    driver = str(documents["parameters"]["drivers"])
    db = documents["parameters"]["database"]
    link =""
    link = adapter+"://"+username+"/"+db+"?driver="+driver+"?Trusted_Connection=yes"
    engine = sal.create_engine(link)
    conn = engine.connect()
    result = conn.execute("select * from details").fetchall()
    return(result)
    
    