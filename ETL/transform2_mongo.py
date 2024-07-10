import json
import re
import os


year_pattern=r"20\d{2}"

def transform():
    types=["robusta","arabica"]
    for bean in types:
        new_data(bean)
        old_data(bean)

def new_data(coffee_type): 
    input_file = f'../2024/{coffee_type}_data2024.json'
    output_file = f'../2024/{coffee_type}_temp.json'
    invalid_file = f'../2024/{coffee_type}_invalid_data.json'
    processed_data=[]
    invalid_data=[]
    with open (input_file,'r',encoding="utf-8") as file_in:
        payload=json.load(file_in)
        
        for bean in payload:
            flag=False
            aux={}
            species=bean.get('species').lower() 
            country=bean.get('country').lower()
            #revisar que solo sea del tipo especificado
            if species!= coffee_type: 
                invalid_data.append(bean)
                print("no the same type")
                continue #si ya tiene un dato invalido no necesita seguir viendo los demas
            if country.strip() == '':
                    invalid_data.append(bean)
                    print("empty country")
                    continue
            aux['bean_id'] = bean.get('bean_id')
            aux['species'] = species
            aux['country'] = country

            # -- revisar que solo sean el año y en formato INT -- #
            h_year=bean.get('harvest_year')
            g_year=bean.get('grading_year')
            h_year_res= re.findall(year_pattern,h_year)
            g_year_res= re.findall(year_pattern,g_year) 
            if h_year_res :
                h_year=h_year_res[0]
            else:
                invalid_data.append(bean)
                print("invalid haverst year")
                continue
            if g_year_res:
                g_year=g_year_res[0]
            else:
                invalid_data.append(bean)
                print("invalid grading year")
                continue

            total_points=bean['grade'].get('total_points')
            try:
                h_year=int(h_year)
                g_year=int(g_year)
                total_points=float(total_points)
            except:
                invalid_data.append(bean)
                print("not a number year")
                continue
            else:
               aux['harvest_year'] = h_year
               aux['grading_year'] =g_year

            aroma=bean['grade'].get('aroma')
            flavor=bean['grade'].get('flavor')
            aftertaste=bean['grade'].get('aftertaste')
            acidity=bean['grade'].get('acidity')
            body_mouth=bean['grade'].get('body/mouthfeel')
            balance=bean['grade'].get('balance')
            uniformity=bean['grade'].get('uniformity')
            clean_cup=bean['grade'].get('clean-cup')
            sweetness=bean['grade'].get('sweetness')
            overall=bean['grade'].get('overall')
            moisture=bean.get('moisture')
            grades=[aroma,flavor,aftertaste,acidity,body_mouth,balance,uniformity,clean_cup,sweetness,overall,total_points,moisture]

            aux['grade'] = {}
            for grad in grades:
                if isinstance(grad,(int,float)):
                    continue
                else:
                    invalid_data.append(bean)
                    flag=True
                    print("not a number grades")
                    break
            if flag:
                continue
            else:
                aux['grade']['aroma']=round(aroma,2)
                aux['grade']['flavor']=round(flavor,2)
                aux['grade']['aftertaste']=round(aftertaste,2)
                aux['grade']['acidity']=round(acidity,2)
                aux['grade']['body/mouthfeel']=round(body_mouth,2)
                aux['grade']['balance']=round(balance,2)
                aux['grade']['uniformity']=round(uniformity,2)
                aux['grade']['clean_cup']=round(clean_cup,2)
                aux['grade']['sweetness']=round(sweetness,2)
                aux['grade']['overall']=round(overall,2)
                aux['grade']['total_points']=round(total_points,2)
                aux['moisture'] = round(moisture, 2)

            processed_data.append(aux)

    with open(output_file, 'w', newline='', encoding="utf-8") as file_out:
        json.dump(processed_data,file_out, ensure_ascii=False, indent=4)
    with open(invalid_file,'w',encoding="utf-8") as inv:
        json.dump(invalid_data,inv,ensure_ascii=False, indent=4)
    
    os.replace(output_file,input_file)

def old_data(coffee_type):
    
    input_file = f'../2018/{coffee_type}_2018_formatted.json'
    output_file = f'../2018/{coffee_type}_temp.json'
    invalid_file = f'../2018/{coffee_type}_invalid_data.json'
    processed_data=[]
    invalid_data=[]
    with open (input_file,'r',encoding="utf-8") as file_in:
        payload=json.load(file_in)
        
        for bean in payload:
            flag=False
            aux={}
            species=bean.get('species').lower() 
            country=bean.get('country').lower()
            #revisar que solo sea del tipo especificado
            if species!= coffee_type: 
                invalid_data.append(bean)
                print("no the same type")
                continue #si ya tiene un dato invalido no necesita seguir viendo los demas
            if country.strip() == '':
                    invalid_data.append(bean)
                    print("empty country")
                    continue


            # -- revisar que solo sean el año y en formato INT -- #
            h_year=bean.get('harvest_year')
            g_year=bean.get('grading_year')
            
            h_year_res= re.findall(year_pattern,h_year)
            g_year_res= re.findall(year_pattern,g_year) 
            if h_year_res :
                h_year=h_year_res[0]
            else:
                invalid_data.append(bean)
                print("invalid haverst year")
                continue
            if g_year_res:
                g_year=g_year_res[0]
            else:
                invalid_data.append(bean)
                print("invalid grading year")
                continue

            try:
                control_id=int(bean.get('control_id'))
                h_year=int(h_year)
                g_year=int(g_year)
            except:
                invalid_data.append(bean)
                print("not a number year")
                continue
            else:
                aux['control_id']= control_id
                aux['species'] = species
                aux['country'] = country
                aux['harvest_year'] = h_year
                aux['grading_year'] =g_year

            aroma=bean['grade'].get('aroma')
            flavor=bean['grade'].get('flavor')
            aftertaste=bean['grade'].get('aftertaste')
            acidity=bean['grade'].get('acidity')
            body_mouth=bean['grade'].get('body/mouthfeel')
            balance=bean['grade'].get('balance')
            uniformity=bean['grade'].get('uniformity')
            clean_cup=bean['grade'].get('clean-cup')
            sweetness=bean['grade'].get('sweetness')
            overall=bean['grade'].get('overall')
            total_points=bean['grade'].get('total_points')
            moisture=bean.get('moisture')
            grades=[aroma,flavor,aftertaste,acidity,body_mouth,balance,uniformity,clean_cup,sweetness,overall,total_points,moisture]

            aux['grade'] = {}
            for grad in grades:
                if isinstance(grad,(int,float)):
                    continue
                else:
                    invalid_data.append(bean)
                    flag=True
                    print("not a number grades")
                    break
            if flag:
                continue
            else:
                aux['grade']['aroma']=round(aroma,2)
                aux['grade']['flavor']=round(flavor,2)
                aux['grade']['aftertaste']=round(aftertaste,2)
                aux['grade']['acidity']=round(acidity,2)
                aux['grade']['body/mouthfeel']=round(body_mouth,2)
                aux['grade']['balance']=round(balance,2)
                aux['grade']['uniformity']=round(uniformity,2)
                aux['grade']['clean_cup']=round(clean_cup,2)
                aux['grade']['sweetness']=round(sweetness,2)
                aux['grade']['overall']=round(overall,2)
                aux['grade']['total_points']=round(total_points,2)
                aux['moisture'] = round(moisture, 2)

            processed_data.append(aux)

    with open(output_file, 'w', newline='', encoding="utf-8") as file_out:
        json.dump(processed_data,file_out, ensure_ascii=False, indent=4)
    with open(invalid_file,'w',encoding="utf-8") as inv:
        json.dump(invalid_data,inv,ensure_ascii=False, indent=4)
    
    os.replace(output_file,input_file)

if __name__=='__main__':
    transform()