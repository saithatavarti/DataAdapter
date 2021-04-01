from sqlalchemy import create_engine,Column,Integer,String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import json
import yaml
import json
from sqlalchemy import create_engine
from fastapi import FastAPI
app = FastAPI()
@app.get("/")
def main_page():
    return {"Hello": "World"}
@app.get("/oracledata/{choose_id}") 
async def get_component(choose_id):
	a = choose_id
	with open(a+".yaml") as file:
		documents = yaml.safe_load(file)
	adapter=str(documents["development"]["adapter"])    
	username=str(documents["development"]["username"])
	password=str(documents["development"]["password"])
	query=str(documents["query"])
	dbs=documents["development"]["database"]
	link=adapter+"://"+username+":"+password+"@localhost:1521/"+dbs
	engine = create_engine(link)
	connection=engine.connect()
	lst = connection.execute(query).fetchall()
	return(lst)


