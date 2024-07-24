import csv
import json
import sys


def main():
    #columnas que son de interés para el análisis
    data=[1,3,15,16,20,21,22,23,24,25,26,27,28,29,30,31,0]
    types=["robusta","arabica"]
    #verifica si hay valores de interés vacíos en el archivo .csv, si los hay se pueden eliminar automaticamente

    for type in types:
        empty_values(type,data)
        parsed_data=values_to_json(type, data)
        with open(f'../2018/{type}_2018_formatted.json', 'w', encoding='utf-8') as file2:
            file2.write('')
            json.dump(parsed_data,file2, ensure_ascii=False, indent=4)  



def empty_values(coffee_type,value):

    empty=[]
    with open (f'raw_csv/{coffee_type}_2018.csv','r', encoding="utf-8") as file:
        reader=csv.reader(file,delimiter=',')
        next(reader)
        for line in reader:
            for val in value:
                if line[val].strip() == '':
                    empty.append(line[0])
                    break #-- si una columna está vacía no verifica las demás
    if empty:
        print(f"Los siguientes IDs del grano {coffee_type} tienen valores vacíos:\n {empty}")
        user=input('se borrarán los granos que tiene elementos vacíos, continuar?(y/n): ')
        if user.lower()=='n':
            sys.exit(0)
        else:
            delete_empty_values(coffee_type,empty)
    else: 
        input_file = f'raw_csv/{coffee_type}_2018.csv'
        output_file = f'../2018/{coffee_type}_2018.csv'
        with open(input_file, 'r', encoding="utf-8") as file_in, \
             open(output_file, 'w', newline='', encoding="utf-8") as file_out:
            file_out.write('')
            reader = csv.reader(file_in)
            writer = csv.writer(file_out)
            for row in reader:
                writer.writerow(row)

def delete_empty_values(coffee_type,empty):

    input_file = f'raw_csv/{coffee_type}_2018.csv'
    output_file = f'../2018/{coffee_type}_2018.csv'
    
    with open(input_file, 'r', encoding="utf-8") as file_in, \
         open(output_file, 'w', newline='', encoding="utf-8") as file_out:
        file_out.write('')
        reader = csv.reader(file_in)
        writer = csv.writer(file_out)
        deleted = 0
        for row in reader:
            if row[0] not in empty:
                writer.writerow(row)
            else:
                deleted += 1

    print(f"Se borraron los siguientes ids de {coffee_type}: {empty}\nEn total fueron {deleted} filas.")


def values_to_json(coffee_type,values):
    with open(f"../2018/{coffee_type}_2018.csv", 'r', encoding="utf-8") as file:
        reader = csv.reader(file)
        next(reader)
        json_file = []
        for line in reader:
            array = []
            for val in values:
                value = line[val].strip()
                if val in [20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]:
                    array.append(float(value) if value else None)
                else:
                    array.append(value)
            
            aux = {
                'control_id': array[16],
                'species': array[0],
                'country': array[1],
                'harvest_year': array[2],
                'grading_year': array[3],
                'grade': {
                    'aroma': array[4],
                    'flavor': array[5],
                    'aftertaste': array[6],
                    'acidity': array[7],
                    'body_mouthfeel': array[8],
                    'balance': array[9],
                    'uniformity': array[10],
                    'clean_cup': array[11],
                    'sweetness': array[12],
                    'overall': array[13],
                    'total_points': array[14]
                },
                'moisture': array[15]
            }
            json_file.append(aux)
    return json_file


if __name__=='__main__':
    main()
