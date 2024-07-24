from neo_db_connection import get_driver
from neo4j.exceptions import Neo4jError

driver=get_driver()


def main():

    try:
        with driver.session(database="neo4j") as session:
         #Obtener los 3 países con mejores putuaciones promedio
            top_countries = session.execute_read(get_top_coffee_countries)
            print("Los países que en promedio obtienen puntuaciones más altas son:")
            for country in top_countries:
                print(f"{country['country']}: {country['averageScore']} puntos")

            # arabica vs robusta
            species_comparison = session.execute_read(compare_species)
            print("\nComparación entre especies de café:")
            for i in species_comparison:
                print(f"\nEspecie: {i['species']}")
                print(f"Número de muestras: {i['count']}")
                print(f"Aroma : {i['avg_aroma']}")
                print(f"Sabor : {i['avg_flavor']}")
                print(f"Regusto : {i['avg_aftertaste']}")
                print(f"Acidez : {i['avg_acidity']}")
                print(f"Cuerpo : {i['avg_body_mouthfeel']}")
                print(f"Balance : {i['avg_balance']}")
                print(f"Uniformidad : {i['avg_uniformity']}")
                print(f"Taza limpia : {i['avg_clean_cup']}")
                print(f"Dulzura : {i['avg_sweetness']}")
                print(f"Overall: {i['avg_overall']}")
                print(f"Puntuación total: {i['avg_total_points']}")

            #analisis de humedad de los granos y la putuacion promedio mas alta
            moisture_analysis = session.execute_read(analyze_moisture)
            print("\nAnálisis de humedad:")
            for record in moisture_analysis:
                print(f"Rango de humedad: {record['moisture_range']}%.  Número de muestras: {record['count']}.  Puntuación promedio: {record['avg_score']}")
            best_category = max(moisture_analysis, key=lambda x: x['avg_score'])
            
            lower, upper = map(int, best_category['moisture_range'].split('-'))
            print(f"Los cafés con mayor puntuación tienden a tener un nivel de humedad entre {lower}% y {upper}%.")

        return
    except Neo4jError as err:
        print (f"An unexpected Neo4j error ocured.\n Error code: {err.code} \n Error message: {err}")
        return


def get_top_coffee_countries(tx, limit=3):
    query = """
    MATCH (c:COUNTRY)<-[:IS_FROM]-(b:BEAN)-[r:HAS_GRADE]->(g:GRADE) 
    WITH c.name AS country, round(AVG(r.total_points),2) AS averageScore, COUNT(b) AS count
    WHERE count > 30
    RETURN country, averageScore, count
    ORDER BY averageScore DESC
    LIMIT $limit
    """
    return list(tx.run(query, {"limit": limit}))

def compare_species(tx):
    query = """
    MATCH (b:BEAN)-[r:HAS_GRADE]->(g:GRADE)
    RETURN b.species AS species,
           round(AVG(g.aroma),2) AS avg_aroma,
           round(AVG(g.flavor),2) AS avg_flavor,
           round(AVG(g.aftertaste),2) AS avg_aftertaste,
           round(AVG(g.acidity),2) AS avg_acidity,
           round(AVG(g.body_mouthfeel),2) AS avg_body_mouthfeel,
           round(AVG(g.balance),2) AS avg_balance,
           round(AVG(g.uniformity),2) AS avg_uniformity,
           round(AVG(g.clean_cup),2) AS avg_clean_cup,
           round(AVG(g.sweetness),2) AS avg_sweetness,
           round(AVG(g.overall),2) AS avg_overall,
           round(AVG(r.total_points),2) AS avg_total_points,
           COUNT(b) AS count
    """
    return list(tx.run(query))

def analyze_moisture(tx):
    query="""
    MATCH (b:BEAN)-[r:HAS_GRADE]->(g:GRADE)
    WITH b.moisture AS moisture, r.total_points AS score
    WITH 
    CASE 
        WHEN moisture < 9 THEN '0-9'
        WHEN moisture < 10 THEN '9-10'
        WHEN moisture < 11 THEN '10-11'
        WHEN moisture < 12 THEN '11-12'
        WHEN moisture < 13 THEN '12-13'
        WHEN moisture < 14 THEN '13-14'
        WHEN moisture < 15 THEN '14-15'
        ELSE '15+'
    END AS moisture_range,
    score
    WITH moisture_range, COUNT(*) AS count, round(AVG(score),2) AS avg_score
    WHERE count > 40
    RETURN 
    moisture_range,
    count,
    avg_score
    ORDER BY avg_score DESC
    """
    return list(tx.run(query))




if __name__=='__main__':
        
    main()
    try:
        driver.close()
    except Exception as e:
        print(f"\nErorr al intentar cerrar el driver. {e}")
    else: print("\nDriver cerrado correctamente")