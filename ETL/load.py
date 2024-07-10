from db_connection import client
from pymongo import errors as er
from bson import errors
import json

#-- database config --#
DB=client.coffee_beans

rating=DB.ratings

#-<----------------->-#


def load():
    types=["robusta","arabica"]
    for type in types:
        with open(f'../2024/{type}_data2024.json', 'r', encoding="utf-8") as file:
            payload=json.load(file)
            err=[]
            for bean in payload:
                temp_id=bean.get('bean_id')
                try:
                    status= rating.insert_one(bean)
                except errors.BSONError or er.PyMongoError:
                    print(f'Error! "{status}" al insertar id: {temp_id}')
                    err.append(temp_id)
                else:
                    print(f"elemento insertado. {status}")
            if err:
                with open(f'{type}_incorrect_load.txt', 'w', encoding="utf-8") as f:
                    f.write(f'2024 {type} beans:\n')
                    for item in err:
                        f.write(item)
                 
        with open(f'../2018/{type}_2018_formatted.json', 'r', encoding="utf-8") as file:
            payload=json.load(file)
            err=[]
            for bean in payload:
                temp_id=bean.get('control_id')
                try:
                    status= rating.insert_one(bean)
                except errors.BSONError or er.PyMongoError:
                    print(f'Error! "{status}" al insertar id: {temp_id}')
                    err.append(temp_id)
                else:
                    print(f"elemento insertado. {status}")
            if err:
                with open(f'{type}_incorrect_load.txt', 'a', encoding="utf-8") as f:
                    f.write(f'2018 {type} beans:\n')
                    for item in err:
                        f.write(item)
if __name__=='__main__':
    load()