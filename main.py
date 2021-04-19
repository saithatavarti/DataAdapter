import yaml
from fastapi import FastAPI
import json
from sqlalchemy import create_engine
from fastapi.encoders import jsonable_encoder

app = FastAPI()

@app.get("/")
def read_root():
    return {"Hello": "World"}

# dynamic method for opting a yaml file 
@app.get("/{username}/{choose_id}/{min}/{offset}")
def get_component(username,choose_id,min,offset):

    us=username
    with open(choose_id+'.yaml') as file:
        documents = yaml.safe_load(file)
    lower=int(min)
    upper=int(offset)
    min=int(min)-1
    max=int(offset)-int(min)
    page=upper-lower+1
    adapter=str(documents["development"]["adapter"])
    username=str(documents["development"]["username"])
    password=str(documents["development"]["password"])
    query=str(documents["query"]["code"])
    dbs=documents["development"]["database"]
    url=str(documents["development"]["url"])
    link=adapter+"://"+username+":"+password+"@localhost:3308/"+dbs+"?charset=utf8mb4"
    
    
    if(max<0 or min<0):
	    return("limit is given wrong")
    else:
	    query1=query+" "+"limit"+" "+str(min)+","+str(max)
	
	    	
    engine = create_engine(link)
    connection=engine.connect()
    userQuery="select * from users where Name='{0}'".format(us)
    print(userQuery)
    usercheck = connection.execute(userQuery).fetchall()
    print(usercheck)
    output= connection.execute(query1).fetchall()
    count= len(connection.execute(query).fetchall())

    if usercheck:
        f=int(count)
        if(f%int(page)==0):
            total_pages=int(f/page)
        else:
            total_pages=int((f)/page)+1
        
        url="next_url=="+url+us+"/"+choose_id+"/"+str(lower+page)+"/"+str(upper+page)
        print(min,max)
        data="data:"
        meta="meta:"
        end="end of pages"

        page_number="page_num:"+str(int(upper/page))
        if((upper/page)>total_pages):
            return("end of pages")
        elif(int(upper/page)==total_pages):
            # return data,output,end,page_number
            return data,output,meta,end,page_number
        else:
            pages="total_pages: "+str(total_pages)
            # vb= jsonable_encoder(data)
            return data,output,meta,url,page_number,pages
    else:
        return "user does not exists"
