import json
from neo_db_connection import get_driver
from neo4j.exceptions import Neo4jError

driver=get_driver()

QUERY= """
    MERGE (c:COUNTRY {name: $country})
    CREATE (b:BEAN {species: $species, moisture: $moisture})
    CREATE (g:GRADE {aroma: $aroma, flavor: $flavor, aftertaste:$aftertaste,acidity: $acidity, body_mouthfeel: $body_mouthfeel,balance: $balance, uniformity: $uniformity, clean_cup: $clean_cup, sweetness: $sweetness, overall: $overall})
    CREATE (b)-[:IS_FROM {harvest_year: $harvest_year}]->(c)
    CREATE (b)-[:HAS_GRADE {grading_year: $grading_year, total_points: $total_points}]->(g)
"""

def main():

    types=["robusta","arabica"]
    all_data=[]
    for type in types:
        with open(f'../2024/{type}_data2024.json', 'r', encoding="utf-8") as file:
            payload1=json.load(file)
            all_data.extend(payload1)
        with open(f'../2018/{type}_2018_formatted.json', 'r', encoding="utf-8") as file:
            payload2=json.load(file)
            all_data.extend(payload2)
    try:
        with driver.session(database="neo4j") as session:
            result, errors=session.execute_write(nodes_and_relations,all_data)
            print(f"Success.\nTotal beans inserted: {result} ")
            if errors:
                print(f"Found {len(errors)} data unable to load.\n")
                for error in errors:
                    print(f"{error}\n")
    except Neo4jError as err:
        print (f"An unexpected Neo4j error ocured and nothing was loaded.\n Error code: {err.code} \n Error message: {err}")
        return
    return


def nodes_and_relations(tx, all_data):
    count=0
    errors=[]
    for index, data in enumerate(all_data):
        try:
            tx.run(QUERY,
                    country=data['country'],
                    species=data['species'],
                    moisture=data['moisture'],
                    harvest_year=data['harvest_year'],
                    grading_year=data['grading_year'],
                    aroma=data['grade']['aroma'],
                    flavor=data['grade']['flavor'],
                    aftertaste=data['grade']['aftertaste'],
                    acidity=data['grade']['acidity'],
                    body_mouthfeel=data['grade']['body_mouthfeel'],
                    balance=data['grade']['balance'],
                    uniformity=data['grade']['uniformity'],
                    clean_cup=data['grade']['clean_cup'],
                    sweetness=data['grade']['sweetness'],
                    overall=data['grade']['overall'],
                    total_points=data['grade']['total_points']
                    )
            count +=1
        except Exception as err:
            errors.append(f"Error en el índice {index}: {err}")

    return count, errors
    

if __name__=='__main__':
        
    main()
    try:
        driver.close()
    except Exception as e:
        print(f"Erorr al intentar cerrar el driver. {e}")
    else: print("Driver cerrado correctamente")