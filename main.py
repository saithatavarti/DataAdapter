import yaml
from fastapi import FastAPI
import json
from sqlalchemy import create_engine

app = FastAPI()

@app.get("/")
def read_root():
    return {"Hello": "World"}

# dynamic method for opting a yaml file 
@app.get("/mariadb/{choose_id}/{min}/{offset}")
def get_component(choose_id,min,offset):
    
    with open(choose_id+'.yaml') as file:
        documents = yaml.safe_load(file)
    lower=int(min)
    upper=int(offset)
    min=int(min)-1
    max=int(offset)-int(min)

    adapter=str(documents["development"]["adapter"])
    username=str(documents["development"]["username"])
    password=str(documents["development"]["password"])
    columns=str(documents["query"]["columns"])
    table=str(documents["query"]["table"])
    where=str(documents["query"]["where"])
    dbs=documents["development"]["database"]
    order=str(documents["query"]["order"])
    group=str(documents["query"]["group"])
    limit=str(documents["query"]["limit"])
    page=int(documents["query"]["page_size"])
    link=adapter+"://"+username+":"+password+"@localhost:3308/"+dbs+"?charset=utf8mb4"
    
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

    if(limit=="None"):
	    max=max
    elif(int(limit)<=max):
	    max=int(limit)
	    max=max-min
    else:
	    max=max
    if(max<0 or min<0):
	    return("limit is given wrong")
    else:
	    query=query+" "+"limit"+" "+str(min)+","+str(max)
	
	

    query1="select "	
    if (columns=="None"):
	    return("column names incorrect")
    else:
	    query1=query1+"count(*) "
    if (table=="None"):
	    return("table doesn't exist")
    else:
	    query1=query1+"from"+" "+table
    if (where== "None"):
	    query1=query1
    else:
	    query1=query1+" "+"where"+" "+where
    if(group=="None"):
	    query1=query1
    else:
	    query1=query1+" "+"group by"+" "+group
    if(order=="None"):
	    query1=query1
    else:
	    query1=query1+" "+"order by"+" "+order
    print(query1)


	
    engine = create_engine(link)
    connection=engine.connect()
    lst = connection.execute(query).fetchall()
    lst1 = connection.execute(query1).fetchall()


    f=int(lst1[0][0])
    if(f%int(page)==0):
	    total_pages=int(f/page)
    else:
	    total_pages=int((f)/page)+1
	
    str1="next_url==http://127.0.0.1:8000/mariadb/dbc/"+str(lower+page)+"/"+str(upper+page)
    print(min,max)
    data="data:"
    meta="meta:"

    if((upper/10)>total_pages):
	    return("end of pages")
    else:
	    pages="total_pages: "+str(total_pages)
	    return data,lst,{meta  ,pages,str1}