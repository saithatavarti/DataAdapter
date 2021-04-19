#Importing required libraries
from fastapi import FastAPI, Query
from sqlalchemy import create_engine
import sqlalchemy as sal
import yaml, json
import pandas as pd
import cx_Oracle
import math
import pyodbc

app = FastAPI()

@app.get("/")
def start():
    return{"Message" : "Please enter a YAML filename in URL to return JSON data"}

with open("requirement_postgresql.yaml") as req:
    document = yaml.safe_load(req)
    db_type = str(document["development"]["db_type"])

    #Returning JSON data if database is SQLite
    if(db_type == "sqlite"):

        #Opening connection to database and returning data in JSON format
        @app.get("/{user}/{file_name}/{offset}/{limit}")
        def read_file(user,file_name,offset,limit):
            my_dict = {"file" : file_name}
            my_value = (my_dict["file"])
            username = user
            str_limit = limit
            int_limit = int(limit)
            str_offset = offset
            int_offset = int(offset)
            offset_increment = int_limit
            try:
                with open(my_value) as req:
                    document = yaml.safe_load(req)
                    db_name = str(document["development"]["database"])
                    authentication_query = str(document["authentication_query"])
                    authentication_query = authentication_query  +  "'" + username + "'" 
                    query = str(document["query"]["code"])
                    query = query +" " + "LIMIT" + " " + str_offset + "," + str_limit
                    count_query = str(document["count_query"])
                    conn_link = "sqlite:///" + db_name + ".db"
                    engine = create_engine(conn_link, connect_args={'check_same_thread': False})
                    connection = engine.connect()
                    result = connection.execute(query).fetchall()
                    authentication_result = connection.execute(authentication_query).fetchall()
                    authentication_result = str(authentication_result[0][0])
                    count = connection.execute(count_query).fetchone()
                    count = int(count[0])
                    total_pages = count/int_limit
                    total_pages = "Total Pages:" + str(math.ceil(total_pages))
                    current_page = int((int_offset/int_limit) + 1)
                    current_page = "Current Page No." + str(current_page)
                    new_offset = int_offset + offset_increment
                    link = "Link to next records : http://127.0.0.1:8000" + "/" + str(my_value) + "/" + str(username) + "/" + str(new_offset) + "/" + str(str_limit)
                    if(int_offset < count):
                        if(username == authentication_result):
                            return (result,link,total_pages,current_page)
                        else:
                            return("User not authenticated")
                    else:
                        return ("No more records to show")
            except:
                return{"Message" : "Please specify correct filename in URL"}


    #Returning JSON data if database is MySQL
    elif(db_type == "mysql"):

        #Opening connection to database and returning data in JSON format
        @app.get("/{user_name}/{file_name}/{min_limit}/{max_limit}")
        def fun(user_name,file_name,min_limit,max_limit):
            file_id=file_name
            user=user_name
            lower=int(min_limit)
            upper=int(max_limit)
            min=int(min_limit)-1
            max=int(max_limit)-int(min)
            with open(file_id+'.yaml') as file:
                documents = yaml.safe_load(file)
            adapter=str(documents["development"]["adapter"])
            username=str(documents["development"]["username"])
            password=str(documents["development"]["password"])
            query=str(documents["query"]["code"])
            user_query=str(documents["query"]["user"])
            db_name=str(documents["development"]["database"])
            page=max
            url=str(documents["development"]["url"])
            link=adapter+"://"+username+":"+password+"@localhost/"+db_name
            
            if(max<0 or min<0):
                return("limit is given wrong")
            else:
                count_query=query+" "+"limit"+" "+str(min)+","+str(max)
            user_query=user_query+"'"+user+"'"
            engine = create_engine(link)
            connection=engine.connect()
            output = connection.execute(count_query).fetchall()
            count = len(connection.execute(query).fetchall())
            authentication=connection.execute(user_query).fetchall()
            if(authentication):
                count=int(count)
                if(count%int(page)==0):
                    total_pages=int(count/page)
                else:
                    total_pages=int(count/page)+1
            
                url="next_url:"+url+user+"/"+file_id+"/"+str(lower+page)+"/"+str(upper+page)
                print(min,max)
                data="data:"
                meta="meta:"
                end="end of pages"
                page_number="page_num:" + str(int(upper/page))
                if((upper/page)>total_pages):
                    return("end of pages")
                elif(int(upper/page)==total_pages):
                    return data,output,meta,end,page_number
                else:
                    pages="total_pages: "+str(total_pages)
                    return data,output,meta,url,page_number,pages
            else:
                return "user does not exist"


    #Returning JSON data if database is MariaDB
    elif(db_type == "mariadb"):

       #Opening connection to database and returning data in JSON format
       @app.get("/{username}/{choose_id}/{min}/{offset}")
       def get_component(username,choose_id,min,offset):
            user =username
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
            user_query=str(documents["query"]["user"])
            db_name=documents["development"]["database"]
            port = str(documents["development"]["port"])
            url=str(documents["development"]["url"])
            link=adapter+"://"+username+":"+password+"@localhost:"+port+"/"+db_name+"?charset=utf8mb4"
            
            
            if(max<0 or min<0):
                return("limit is given wrong")
            else:
                data_query=query+" "+"limit"+" "+str(min)+","+str(max)
            
                    
            engine = create_engine(link)
            connection=engine.connect()
            userQuery = user_query+"'"+user+"'"
            print(userQuery)
            usercheck = connection.execute(userQuery).fetchall()
            print(usercheck)
            output= connection.execute(data_query).fetchall()
            count= len(connection.execute(query).fetchall())

            if usercheck:
                f=int(count)
                if(f%int(page)==0):
                    total_pages=int(f/page)
                else:
                    total_pages=int((f)/page)+1
                
                url="next_url=="+url+user+"/"+choose_id+"/"+str(lower+page)+"/"+str(upper+page)
                print(min,max)
                data="data:"
                meta="meta:"
                end="end of pages"

                page_number="page_num:"+str(int(upper/page))
                if((upper/page)>total_pages):
                    return("end of pages")
                elif(int(upper/page)==total_pages):
                    return data,output,meta,end,page_number
                else:
                    pages="total_pages: "+str(total_pages)
                    return data,output,meta,url,page_number,pages
            else:
                return "user does not exists"


    #Returning JSON data if database is Oracle               
    elif(db_type == "oracle"):

        #Opening connection to database and returning data in JSON format
        @app.get("/{user_name}/{choose_id}/{min}/{max}") 
        async def get_component(user_name,choose_id,min,max):
            file_name = choose_id
            lower=int(min)-1
            upper=max
            user=user_name
            with open(file_name+".yaml") as file:
                documents = yaml.safe_load(file)
            adapter=str(documents["development"]["adapter"])    
            username=str(documents["development"]["username"])
            password=str(documents["development"]["password"])
            # pagesize=str(documents["development"]["pagesize"])
            db_name=documents["development"]["database"]
            port=documents["development"]["port"]
            url_link=documents['development']['url']
            print(url_link)
            query=str(documents["query"]['code'])
            user_query=str(documents["query"]["user"])
            print(user_query)
            print('the output of query is {}'.format(query))
            query_split=query.split()
            if('group' in query_split):
                count_query=query.replace('*','count(count(*))')
            else:
                count_query=query.replace("*","count(*)")
            print(count_query)
            link=adapter+"://"+username+":"+password+"@localhost:"+str(port)+"/"+db_name
            engine = create_engine(link)
            connection=engine.connect()
            userQuery = user_query+"'"+user+"'"
            usercheck = connection.execute(userQuery).fetchall()
            total_count= connection.execute(count_query).fetchall()
            print(count_query)
            if usercheck:
                if (('where' in query_split) or ('group by' in query_split)):
                    # query='select * from employee'
                    minus_query='select * from ({}) where rownum<={} minus select * from ({}) where rownum<={}'.format(query,upper,query,lower)
                else:
                    # query='select * from employee'
                    minus_query=query+' where rownum<={} minus {} where rownum<={}'.format(int(upper),query,lower)
                print('the query string is {}'.format(query))
                print('the value of e is {} and total colunt is {} '.format(upper,total_count[0][0]))
                if (int(upper)>=int(total_count[0][0])):
                    nxt="end of pages "
                else:
                    nxt="{}{}/{}/{}/{}".format(url_link,user,file_name,int(upper)+1,int(upper)+(int(upper)-int(lower)))
                # else:
                # 	nxt="{}/{}/{}/{}/{}".format(url_link,user,a,int(e)+1,total_count[0][0])
                result_data = connection.execute(minus_query).fetchall()
                print(total_count)
                print(result_data)
                current_page=int((int(upper)/(int(upper)-int(lower))))
                print("current_page {}".format(current_page))
                page_no="current page:{}".format(current_page)
                data="data:"
                meta="meta:"
                next_url="next_link:{}".format(nxt)
                total_pages="total_pages:{}".format(int(1+(int(total_count[0][0])/(int(upper)-int(lower)))))
                return data,result_data,meta  ,page_no,total_pages,next_url
                # return next_url,nxt,data,result_data,total_pages,total_count[0][0]
            else:
                return "enter correct user name"


    #Returning JSON data if database is PostgreSQL            
    elif(db_type == "postgresql"):

        #Opening connection to database and returning data in JSON format
          @app.get("/{user_name}/{choose_id}/{minimum}/{offset}")
          async def get_component(user_name,choose_id,minimum,offset):
            url = choose_id
            user = user_name
            lower= int(minimum)
            upper= int(offset)
            min = int(offset)-int(minimum)+1
            max = int(minimum)-1
            page = int(upper-lower+1)
            
            with open(url+".yaml") as file:
                documents = yaml.safe_load(file)
            adapter = str(documents["development"]["adapter"])    
            username = str(documents["development"]["username"])
            password = str(documents["development"]["password"])
            db_name = documents["development"]["database"]
            port = str(documents["development"]["port"])
            address = str(documents["development"]["url"])
            query = str(documents["query"]["code"])
            user_query = str(documents["query"]["user"])
            link =""
            link = adapter+"://"+username+":"+password+"@localhost:"+port+"/"+db_name

            if(max<0 or min<1):
                return("limit is given wrong")
            else:
                query_limit = query+" "+"limit"+" "+str(min)+" offset "+str(max)

           
            
            engine = create_engine(link,echo=False)
            connection=engine.connect()
            userQuery = user_query+"'"+user+"'"
            print (userQuery)
            usercheck = connection.execute(userQuery).fetchall()
            print(usercheck)

            if usercheck:
                
                result = connection.execute(query_limit).fetchall()
                result_len = len(connection.execute(query).fetchall())
                result_len_count = int(result_len)
                if(result_len_count %int(page)==0):
                    total_pages=int(result_len_count /page)
                else:
                    total_pages=int((result_len_count )/page)+1
                
                NextUrl ="next_url== "+address+url+"/"+user+"/"+str(lower+page)+"/"+str(upper+page)
                print(min,max)
                data= "data:"
                meta= "meta:"
                pageNo = "pagenumber:" + str(int(upper/page))
                end = "end of pages"

                if((upper/page)>total_pages):
                    return("end of pages")
                elif((upper/page) == total_pages):
                    return data,result,meta ,end,pageNo
                else:
                    return data,result,meta,NextUrl,pageNo,total_pages
            else:
                return "User Does not Exists"


    #Returning JSON data if database is MSSQL            
    elif(db_type == "mssql"):

        #Opening connection to database and returning data in JSON format
        @app.get("/{username}/{choose_id}/{min}/{offset}")
        def test(username,choose_id,min,offset):
            user = username
            url = choose_id
            lower=int(min)
            upper=int(offset)
            min=int(min)-1
            max=int(offset)-int(min)
            page = int(upper-lower+1)

            with open(choose_id+".yaml") as file:
                documents = yaml.safe_load(file)
            adapter = str(documents["development"]["adapter"])
            username = str(documents["development"]["username"])
            driver = str(documents["development"]["drivers"])
            db_name = documents["development"]["database"]
            address = str(documents["development"]["url"])
            query = str(documents["query"]["code"])
            user_query=str(documents["query"]["user"])
            print(query)
            query_min = query.replace("*","top {} * ".format(min))
            print(query_min )
            query_offset = query.replace("*","top {} * ".format(offset))
            print(query_offset )
            finalquery="{}  EXCEPT {}".format(query_offset ,query_min )
            print(finalquery)
            link =""
            link = adapter+"://"+username+"/"+db_name+"?driver="+driver+"?Trusted_Connection=yes"

            
            engine = sal.create_engine(link)
            conn = engine.connect()
            user_query = user_query+"'"+user+"'"

            usercheck = conn.execute(user_query).fetchall()
            print(usercheck)

            if usercheck:
                result = conn.execute(finalquery).fetchall()
                result_len = len(conn.execute(query).fetchall())
                result_len_count = int(result_len)
                if(result_len_count%int(page)==0):
                    total_pages=int(result_len_count/page)
                else:
                    total_pages=int((result_len_count)/page)+1

                NextUrl ="next_url== "+address+url+"/"+str(lower+page)+"/"+str(upper+page)
                print(min,max)
                data= "Data:"
                meta= "Meta:"
                end = "End Of Pages"
                pageNo = "Page Number: " + str(int(upper/page))

                if((upper/page)>total_pages):
                    return("end of pages")
                elif((upper/page) == total_pages):
                    return data,result,meta ,pageNo,end
                else:
                    return data,result,meta,NextUrl,pageNo,total_pages
            else:
                return "User Does not Exists"
