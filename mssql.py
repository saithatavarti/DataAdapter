# microsoft sql

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


@app.get("/{choose_id}/{min}/{offset}")
def test(choose_id,min,offset):
    url = choose_id
    lower=int(min)
    upper=int(offset)
    min=int(min)-1
    max=int(offset)-int(min)
    page = int(upper-lower+1)

    with open(choose_id+".yaml") as file:
        documents = yaml.safe_load(file)
    adapter = str(documents["parameters"]["adapter"])
    username = str(documents["parameters"]["username"])
    driver = str(documents["parameters"]["drivers"])
    db = documents["parameters"]["database"]
    address = str(documents["parameters"]["address"])
    query=str(documents["query"])
    print(query)
    query2=query.replace("*","top {} * ".format(min))
    print(query2)
    query3=query.replace("*","top {} * ".format(offset))
    print(query3)
    finalquery="{}  EXCEPT {}".format(query3,query2)
    print(finalquery)
    link =""
    link = adapter+"://"+username+"/"+db+"?driver="+driver+"?Trusted_Connection=yes"

    
    engine = sal.create_engine(link)
    conn = engine.connect()
    lst = conn.execute(finalquery).fetchall()
    lst1 = len(conn.execute(query).fetchall())
    f = int(lst1)
    if(f%int(page)==0):
        total_pages=int(f/page)
    else:
        total_pages=int((f)/page)+1

    str1="next_url== "+address+url+"/"+str(lower+page)+"/"+str(upper+page)
    print(min,max)
    data= "data: "
    meta= "meta: "


    if((upper/page)>total_pages):
        return("end of pages")
    else:
        return data,lst,{meta , total_pages,str1}

    

    
    