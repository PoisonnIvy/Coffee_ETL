import requests
import re
import json
import time
from db_connection import client

##Database details

main=client.coffee  #base de datos

robusta=main.robusta    #colección de granos robusta
arabica=main.arabica    #colección de grans arábica

###########



#Obtiene solo los ids que estan en el archivo de texto y los guarda en el array "id"

def get_id():

    pattern=r"#(\d+)"
    with open('robusta_ids.txt', 'r', encoding="utf-8") as text:
        content = text.read()
        id = re.findall(pattern, content)
    return id

#Llama a la api con el id del grano para que retorne un json con toda la info de su calificación
def fetch_data(coffee_id):
    url=f'https://database.coffeeinstitute.org/api/coffee/random/{coffee_id}'
    res=requests.get(url)
    if res.status_code==200:
        return res.json()
    else:
        print(f'Error fetching data for ID {coffee_id}: status code {res.status_code}')
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
            'aroma':data['grade'].get('aroma',{}),
            'flavor':data['grade'].get('flavor',{}),
            'aftertaste':data['grade'].get('after',{}),
            'acidity':data['grade'].get('acidity',{}),
            'body/mouthfeel':round(
                data.get('grade').get('body',{}) 
                if data.get('grade').get('body',{}) != 0 
                else data.get('grade').get('mouthfeel', 0), 1
            ),
            'balance':data['grade'].get('balance',{}),
            'uniformity':data['grade'].get('uniformity',{}),
            'clean-cup':data['grade'].get('clean',{}),
            'sweetness':data['grade'].get('sweet',{}),
            'overall':data['grade'].get('aroma',{}),
            'total_points':data['grade'].get('total',{}),

        }
    }

def transform_data(data):
    return

def load_data(data):
    return

def run_ETL(data, functions):

    for func in functions:
        data= func(data)
    return data

def main():
    ids=get_id()
    clean_data=[]
    '''functions=[
        extract_relevant_data(),
        #transform_data(),
        #load_data()
    ]'''

    for id in ids:
        data=fetch_data(id)
        if data:
            processed_data=extract_relevant_data(data)
            #run_ETL(data, functions)
            clean_data.append(processed_data)
        else: 
            print(f'No data with id {id} found, proceeding with the next one...')
        time.sleep(1)

    with open('../robusta_data2024', 'w', encoding='utf-8') as file:
        file.write('')
        json.dump(clean_data,file, ensure_ascii=False, indent=4)

if __name__=='__main__':
    main()
    load()