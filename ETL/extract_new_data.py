import json
import time
import re
import requests


def main():
    clean_data=[]
    unavailable_ids=[]
    type=["robusta","arabica"]
    for x in type:
        bean_id=find_all(f"raw_json_ids/{x}_ids.txt")
        for ids in bean_id:
            data=fetch_data(ids)
            if data:
                processed_data=extract_relevant_data(data)
                clean_data.append(processed_data)
            else: 
                unavailable_ids.append(ids)
            time.sleep(1)
        if unavailable_ids:
            with open(f'../2024/{x}_unavailable.txt', 'w', encoding="utf-8") as u:
                u.write('')
                for ids in unavailable_ids:
                    u.write(f"{ids}\n")
        with open(f'../2024/{x}_data2024.json', 'w', encoding='utf-8') as file:
            file.write('')
            json.dump(clean_data,file, ensure_ascii=False, indent=4)
        clean_data=[]
        unavailable_ids=[]


def find_all(bean_path:str):

    pattern=r"#(\d+)"

    with open(bean_path, 'r', encoding="utf-8") as text:
        content = text.read()
        id = re.findall(pattern, content)
    return id


#Llama a la api con el id del grano para que retorne un json con toda la info de su calificación
def fetch_data(coffee_id:int):
    url=f'https://database.coffeeinstitute.org/api/coffee/random/{coffee_id}'

    try:
        res=requests.get(url=url)
        res.raise_for_status()
        data=res.json()
         
        if 'code' in data and data ['code']==403:
            print(f"ID {coffee_id} no disponible: {data['message']}")
            return None
        else:
            return (data)
    except requests.exceptions.RequestException as e:
        print(f"Error al obtener datos para ID {coffee_id}: {e}")
        return None
    

#Extrae solo los datos necesarios ya definidos 
def extract_relevant_data(data):

    return{
        'bean_id': data.get('random_id',{}),
        'species':data.get('species_title',{}),
        'country':data.get('origin_title',{}),
        'harvest_year':data.get('harvest',{}),
        'grading_year':data.get('completed_desc',{}),
        'grade':{
            'aroma':data.get('grade').get('aroma',{}),
            'flavor':data.get('grade').get('flavor',{}),
            'aftertaste':data.get('grade').get('after',{}),
            'acidity':data.get('grade').get('acidity',{}),
            'body_mouthfeel':(
                data.get('grade').get('body',{}) 
                if data.get('grade').get('body',{}) != 0 
                else data.get('grade').get('mouthfeel', 0)
                ),
            'balance':data.get('grade').get('balance',{}),
            'uniformity':data.get('grade').get('uniformity',{}),
            'clean_cup':data.get('grade').get('clean',{}),
            'sweetness':data.get('grade').get('sweet',{}),
            'overall':data.get('grade').get('overall',{}),
            'total_points':data.get('grade').get('total',{}),
        },
        'moisture':data.get('green').get('moisture',{})
    }

if __name__=='__main__':
    main()
