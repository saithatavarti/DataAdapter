
import yaml
from fastapi import FastAPI,Response
app = FastAPI()


@app.get("/")
def read_root():
    return ({"hello"})


@app.get("/items/{file}")
def fun(file):
	f=file
	import yaml

	with open(f+'.yaml') as file:
		documents = yaml.safe_load(file)
	adapter=str(documents["development"]["adapter"])
	username=str(documents["development"]["username"])
	password=str(documents["development"]["password"])
	columns=str(documents["query"]["columns"])
	table=str(documents["query"]["table"])
	limit=str(documents["query"]["limit"])
	where=str(documents["query"]["where"])
	dbs=documents["development"]["database"]
	order=str(documents["query"]["order"])
	group=str(documents["query"]["group"])
	link=adapter+"://"+username+":"+password+"@localhost/"+dbs
	query="select "	
	if (columns=="None"):
		return("column names incorrect")
	else:
		query=query+columns+" "
	if (table=="None"):
		return("table doesn't exist")
	else:
		query=query+"from"+" "+table
	if (where== "None"):
		query=query
	else:
		query=query+" "+"where"+" "+where
	if(group=="None"):
		query=query
	else:
		query=query+" "+"group by"+" "+group
	if(order=="None"):
		query=query
	else:
		query=query+" "+"order by"+" "+order
	if (limit=="None"):
		query=query
	else:
		query=query+" "+"limit"+" "+limit
	


	import json
	from sqlalchemy import create_engine
	engine = create_engine(link)
	connection=engine.connect()
	lst = connection.execute(query).fetchall()
	
	return(lst)
	
    
