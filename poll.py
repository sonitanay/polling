import asyncio
import requests as req
import mysql.connector
import queue
import pandas as pd
from sqlalchemy import create_engine


def updateTable(dictionary, key, conn):
    try:
        dictionary[key].to_sql(key, conn,if_exists= 'append')
        print("\n\t",key,"updated succesfully\n")
    except Exception as e:
        print("\n\t",key,"update failed\n")

async def getDataOne(q):
    
    try:
        response = req.get("https://www.thecocktaildb.com/api/json/v1/1/random.php")
        data = response.json()
        q.put(data)
    except Exception as e:
        print("No response object found for 1st API")


async def getDataTwo(q):
    try:
        response = req.get("https://randomuser.me/api/")
        data = response.json()
        q.put(data)
    except Exception as e:
        print("No response object found for 2nd API")
    

async def main():
    counter = 1
    run_once = True
    while True:
        que = queue.SimpleQueue()
        df = {}
        await asyncio.gather(getDataOne(que), getDataTwo(que))
        while que.empty() is False:
            data = que.get()
            for key in data.keys():
                df[key] = pd.json_normalize(data[key])
         
        engine = create_engine("mysql+pymysql://general:passwd@localhost/internship")
        with engine.connect() as conn, conn.begin():
            for key in df.keys():
                print("\'"+ key +'\' table updating...')
                updateTable(df, key, conn)
        print('\n\t',counter,"cycle(s) completed\n")
        counter += 1
        await asyncio.sleep(5)

    

if __name__ == '__main__':
    asyncio.run(main(), debug=False)