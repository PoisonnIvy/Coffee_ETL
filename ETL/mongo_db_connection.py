from dotenv import load_dotenv, find_dotenv
import os
from pymongo import MongoClient 

load_dotenv(find_dotenv())

password = os.environ.get("PASSWORD")

CONECCTION_STRING= f"mongodb+srv://ETL-NR:{password}@bddnr.rlgylak.mongodb.net/?retryWrites=true&w=majority&appName=BDDNR"

client =MongoClient(CONECCTION_STRING)


