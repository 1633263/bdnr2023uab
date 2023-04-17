
from pymongo import MongoClient
import pandas as pd
import argparse

################################## PARAMETRES DE CONNEXIÓ ###################################

Host = 'localhost' 
Port = 27017

###################################### CONNEXIÓ ##############################################

DSN = "mongodb://{}:{}".format(Host,Port)
conn = MongoClient(DSN)

############################# TRANSFERÈNCIA DE DADES AMB MONGO ##############################

bd = conn['projecte']
try:col=bd.create_collection('coleccions')
except:col=bd['coleccions']
try:edit=bd.create_collection('editorials')
except:edit=bd['editorials']
try:publi=bd.create_collection('publicacions')
except:publi=bd['publicacions']
try:artist=bd.create_collection('artistes')
except:artist=bd['artistes']
try:pers=bd.create_collection('personatges')
except:pers=bd['personatges']
parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file', type=str)
parser.add_argument('-delete_all','--bd',type=str)
args = parser.parse_args()
if args.bd==None:
    try:
        coleccions={}
        editorials={}
        publicacions=[]
        artistes=[]
        personatges={}
        coleccions_agg=(list(col.find()))
        lista_ids_col = [_dict['_id'] for _dict in coleccions_agg]
        editorials_agg=(list(edit.find()))
        lista_ids_edit = [_dict['_id'] for _dict in editorials_agg]
        personatges_agg=list(pers.find())
        lista_ids_pers = [_dict['_id'] for _dict in personatges_agg]
        publicacions_agg=list(publi.find())
        lista_ids_publi = [_dict['_id'] for _dict in publicacions_agg]
        artistes_agg=list(artist.find())
        lista_ids_artist = [_dict['_id'] for _dict in artistes_agg]
        if args.file is not None:
            excel_col=pd.read_excel(args.file,sheet_name='Colleccions-Publicacions',header=0)
        excel_def=list(excel_col.columns)
        data_col = []
        for i, row in excel_col.iterrows():
            if i == 0:
                continue
            dict_row = {}
            for j, value in enumerate(row):
                dict_row[excel_def[j]] = value
            data_col.append(dict_row)
        
        for publicacio in data_col:

            if publicacio['NomEditorial'] not in lista_ids_edit:
                editorial_prov={}
                if publicacio['NomEditorial'] not in editorials:
                    editorial_prov['_id']=publicacio['NomEditorial']
                    editorial_prov['responsable']=publicacio['resposable']
                    editorial_prov['adreca']=publicacio['adreca']
                    editorial_prov['pais']=publicacio['pais']
                    editorial_prov['coleccions']=[publicacio['NomColleccio']]
                    editorials[publicacio['NomEditorial']]=editorial_prov
                else:
                    if publicacio['NomColleccio'] not in editorials[publicacio['NomEditorial']]['coleccions']:
                        editorials[publicacio['NomEditorial']]['coleccions'].append(publicacio['NomColleccio'])
                    
            if publicacio['NomColleccio'] not in lista_ids_col:
                coleccio_prov={}
                if publicacio['NomColleccio'] not in coleccions:
                    coleccio_prov['_id']=publicacio['NomColleccio']
                    coleccio_prov['total_exemplars']=publicacio['total_exemplars']
                    coleccio_prov['genere']=publicacio['genere'][1:len(publicacio['genere'])-1].split(', ')
                    coleccio_prov['idioma']=publicacio['idioma']
                    coleccio_prov['any_inici']=publicacio['any_inici']
                    coleccio_prov['any_fi']=publicacio['any_fi']
                    coleccio_prov['tancada']=publicacio['tancada']
                    coleccions[publicacio['NomColleccio']]=coleccio_prov
            
            if publicacio['ISBN'] not in lista_ids_publi:
                publicacio_prov={}
                publicacio_prov['_id']=publicacio['ISBN']
                publicacio_prov['titol']=publicacio['titol']
                publicacio_prov['stock']=publicacio['stock']
                publicacio_prov['autor']=publicacio['autor']
                publicacio_prov['preu']=publicacio['preu']
                publicacio_prov['num_pagines']=publicacio['num_pagines']
                publicacio_prov['guionistes']=publicacio['guionistes']
                publicacio_prov['dibuixants']=publicacio['dibuixants']
                publicacio_prov['id_coleccio']=publicacio['NomColleccio']
                publicacions.append(publicacio_prov)
        
        if args.file is not None:
            excel_pers=pd.read_excel(args.file,sheet_name='Personatges',header=0)
        excel_def=list(excel_pers.columns)
        data_pers = []
        for i, row in excel_pers.iterrows():
            if i == 0:
                continue
            dict_row = {}
            for j, value in enumerate(row):
                dict_row[excel_def[j]] = value
            data_pers.append(dict_row)
        
        for persona in data_pers:
            if persona['nom'] not in lista_ids_pers:
                persona_prov={}
                if persona['nom'] not in personatges:
                    persona_prov['_id']=persona['nom']
                    persona_prov['tipus']=persona['tipus']
                    persona_prov['isbn']=[persona['isbn']]
                    personatges[persona['nom']]=persona_prov
                else:
                    personatges[persona['nom']]['isbn'].append(persona['isbn'])
        
        if args.file is not None:
            excel_artist=pd.read_excel(args.file,sheet_name='Artistes',header=0)
        excel_def=list(excel_artist.columns)
        data_artist = []
        for i, row in excel_artist.iterrows():
            if i == 0:
                continue
            dict_row = {}
            for j, value in enumerate(row):
                dict_row[excel_def[j]] = value
            data_artist.append(dict_row)
        
        for artista in data_artist:
            if artista['Nom_artistic'] not in lista_ids_artist:
                artista_prov={}
                artista_prov['_id']=artista['Nom_artistic']
                artista_prov['nom']=artista['nom']
                artista_prov['cognoms']=artista['cognoms']
                artista_prov['data_naix']=artista['data_naix'].to_pydatetime().isoformat()
                artista_prov['pais']=artista['pais']
                artistes.append(artista_prov)
        
        coleccions_list=[]
        for key in coleccions:
            coleccions_list.append(coleccions[key])
        editorials_list=[]
        for key in editorials:
            editorials_list.append(editorials[key])
        personatges_list=[]
        for key in personatges:
            personatges_list.append(personatges[key])
        
        if len(coleccions_list)>0:
            col.insert_many(coleccions_list)
        if len(editorials_list)>0:
            edit.insert_many(editorials_list)
        if len(personatges_list)>0:
            pers.insert_many(personatges_list)
        if len(publicacions)>0:
            publi.insert_many(publicacions)
        if len(artistes)>0:
            artist.insert_many(artistes)
    except:
        print("Nom de l'arxiu erroni")
else:
        collection_list = bd.list_collection_names()
        for collection in collection_list:
            bd[collection].drop()

conn.close()
    

        