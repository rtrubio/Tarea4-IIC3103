import requests
import pandas as pd
import xml.etree.ElementTree as ET
import gspread
from gspread_dataframe import set_with_dataframe

#url = 'http://tarea-4.2021-1.tallerdeintegracion.cl/gho_[Código pais].xml'
#r = requests.get(url)

#codigos_paises = ['CHL', 'ZAF', 'DNK', 'JPN', 'USA', 'AUS']

# OBTENER LOS DATOS DE TODOS LOS PAISES

print('voy a empezar con los requests')
r = requests.get('http://tarea-4.2021-1.tallerdeintegracion.cl/gho_CHL.xml')
r1 = requests.get('http://tarea-4.2021-1.tallerdeintegracion.cl/gho_ZAF.xml')
r2 = requests.get('http://tarea-4.2021-1.tallerdeintegracion.cl/gho_DNK.xml')
r3 = requests.get('http://tarea-4.2021-1.tallerdeintegracion.cl/gho_JPN.xml')
r4 = requests.get('http://tarea-4.2021-1.tallerdeintegracion.cl/gho_USA.xml')
r5 = requests.get('http://tarea-4.2021-1.tallerdeintegracion.cl/gho_AUS.xml')

print('hice todas las requests')


df_cols = ["COUNTRY", "YEAR", "GHO", "GHECAUSES", "SEX", "AGEGROUP", "Display", "Numeric", "Low", "High"]

#frames = []
### ------------------------ FORMA 2 ------------------------ ###
def parse_XML(xml_file, df_cols):   
#    xtree = et.parse(xml_file)
#    xroot = xtree.getroot()
    root = ET.fromstring(xml_file)
    rows = []
    
    for node in root: 
        res = []
        #res.append(node.attrib.get(df_cols[0]))
        res.append(node.find(df_cols[0]).text)
        for el in df_cols[1:]: 
            if node is not None and node.find(el) is not None:
                res.append(node.find(el).text)
            else: 
                res.append(None)
        rows.append({df_cols[i]: res[i] 
                     for i, _ in enumerate(df_cols)})
 
    out_df = pd.DataFrame(rows, columns=df_cols)
    
    #frames.append(out_df)

    return out_df


# GENERAR DATAFRAMES DE TODOS LOS PAISES

chl_df = parse_XML(r.text, df_cols)
print('termine parse chile')
zaf_df = parse_XML(r1.text, df_cols)
print('termine parse sudafrica')
dnk_df = parse_XML(r2.text, df_cols)
print('termine parse dinamarca')
jpn_df = parse_XML(r3.text, df_cols)
print('termine parse japon')
usa_df = parse_XML(r4.text, df_cols)
print('termine parse usa')
aus_df = parse_XML(r5.text, df_cols)
print('termine todos los parsing')

#final_df = pd.concat(frames)
final_df = pd.concat([chl_df, zaf_df, dnk_df, jpn_df, usa_df, aus_df])
print('concatene los df')

# ACCES GOOGLE SHEET PARA TODOS LOS PAISES

gc = gspread.service_account(filename='tdi-4-316623-875c6027a34a.json')
sh = gc.open_by_key('10h2pJEPPO5gXjVb2I4u-Hij5I07hUPpfbbKpDt4vruI')
worksheet = sh.get_worksheet(0)
#worksheet1 = sh.get_worksheet(1)
#worksheet2 = sh.get_worksheet(2)
#worksheet3 = sh.get_worksheet(3)
#worksheet4 = sh.get_worksheet(4)
#worksheet5 = sh.get_worksheet(5)

# 1EA5kpoUNukf4hw5smzDlMjWyMv-mh0LtyDEdyI9WIu4

# APPEND DATA TO SHEET DE TODOS LOS PAISES

set_with_dataframe(worksheet, final_df)
#set_with_dataframe(worksheet1, aux1)
#set_with_dataframe(worksheet2, aux2)
#set_with_dataframe(worksheet3, aux3)
#set_with_dataframe(worksheet4, aux4)
#set_with_dataframe(worksheet5, aux5)
print('listo')