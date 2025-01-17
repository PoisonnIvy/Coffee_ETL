from mongo_db_connection import client

DB=client.coffee_beans
collection=DB.ratings

def main():

    top_countries = get_top_coffee_countries()
    print("Los países que en promedio obtienen puntuaciones mas altas son:")
    for country in top_countries:
        print(f"{country['_id']}: {country['averageScore']:.2f} puntos")

    comparison = compare_species()
    print("Comparación entre especies de café:")
    for species in comparison:
        print(f"\nEspecie: {species['_id']}")
        print(f"Número de muestras: {species['count']}")
        print(f"Aroma: {round(species['avg_aroma'],2)}")
        print(f"Sabor: {round(species['avg_flavor'],2)}")
        print(f"Regusto: {round(species['avg_aftertaste'],2)}")
        print(f"Acidez: {round(species['avg_acidity'],2)}")
        print(f"Cuerpo: {round(species['avg_body'],2)}")
        print(f"Balance: {round(species['avg_balance'],2)}")
        print(f"Uniformidad: {round(species['avg_uniformity'],2)}")
        print(f"Clean-cup: {round(species['avg_clean_cup'],2)}")
        print(f"Dulzura: {round(species['avg_sweetness'],2)}")
        print(f"Overall: {round(species['avg_overall'],2)}")
        print(f"Puntuación total: {round(species['avg_total_points'],2)}")

    moisture_analysis = analyze_moisture()
    best_category = max(moisture_analysis, key=lambda x: x['avg_score'])

    if best_category['_id'] == 0:
        print("Los cafés con mayor puntuación tienden a tener un nivel de humedad menor al 5%.")
    else:
        print(f"Los cafés con mayor puntuación tienden a tener un nivel de humedad entre {best_category['_id']}% y {best_category['_id']+1}%.\n")
    


def get_top_coffee_countries(limit=3):
    pipeline = [
        {
            "$group": {
                "_id": "$country",
                "averageScore": {"$avg": "$grade.total_points"},
                "count":{"$sum": 1}
            }
        },
        {
            "$match":{
                "count": {"$gt": 30}
            }
        },
        {
            "$sort": {"averageScore": -1}
        },
        {
            "$limit": limit
        }
    ]
    
    results = list(collection.aggregate(pipeline))
    return results


def compare_species():
    pipeline = [
        {
            "$group": {
                "_id": "$species",
                "avg_aroma": {"$avg": "$grade.aroma"},
                "avg_flavor": {"$avg": "$grade.flavor"},
                "avg_aftertaste": {"$avg": "$grade.aftertaste"},
                "avg_acidity": {"$avg": "$grade.acidity"},
                "avg_body": {"$avg": "$grade.body/mouthfeel"},
                "avg_balance": {"$avg": "$grade.balance"},
                "avg_uniformity": {"$avg": "$grade.uniformity"},
                "avg_clean_cup": {"$avg": "$grade.clean_cup"},
                "avg_sweetness": {"$avg": "$grade.sweetness"},
                "avg_overall": {"$avg": "$grade.overall"},
                "avg_total_points": {"$avg": "$grade.total_points"},
                "count": {"$sum": 1}
            }
        }
    ]
    
    results = list(collection.aggregate(pipeline))
    return results


def analyze_moisture():
    pipeline = [
        {
            "$bucket": {
                "groupBy": "$moisture",
                "boundaries": [0,9,10, 11, 12, 13, 14, 15, 100],
                "default": "Unknown",
                "output": {
                    "count": {"$sum": 1},
                    "avg_score": {"$avg": "$grade.total_points"},
                    "min_score": {"$min": "$grade.total_points"},
                    "max_score": {"$max": "$grade.total_points"}
                }
            }
        },
        {
            "$match":{"count":{"$gt":40}}
        },
        {
            "$sort": {"avg_score": 1}
        }
    ]
    
    results = list(collection.aggregate(pipeline))
    return results


if __name__=='__main__':
    main()