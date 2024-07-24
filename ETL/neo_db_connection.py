import dotenv
import os
from neo4j import GraphDatabase

load_status = dotenv.load_dotenv("../Neo4j-902ea9fa-Created-2024-07-20.txt")
if load_status is False:
    raise RuntimeError('Environment variables not loaded.')

URI = os.getenv("NEO4J_URI")
AUTH = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))

def get_driver():
    driver = GraphDatabase.driver(URI, auth=AUTH)
    driver.verify_connectivity()
    print("Connection established.")
    return driver