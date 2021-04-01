
import yaml
from fastapi import FastAPI
import json
from sqlalchemy import create_engine

app = FastAPI()

@app.get("/")
def read_root():
    return {"Hello": "World"}

# dynamic method for opting a yaml file 
@app.get("/mariadata/{choose_id}")
async def get_component(choose_id):
    with open(choose_id) as file:
        documents = yaml.safe_load(file)
    adapter=str(documents["development"]["adapter"])    
    username=str(documents["development"]["username"])
    password=str(documents["development"]["password"])
    query=str(documents["query"])
    dbs=documents["development"]["database"]
    port=str(documents["development"]["port"])
    engine = create_engine(adapter+"://"+username+":"+password+"@localhost:3308/"+dbs+"?charset=utf8mb4")
    
    connection=engine.connect()
    lst = connection.execute(query).fetchall()
    return(lst)