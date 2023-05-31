
from pymongo import MongoClient
import pandas as pd
import argparse

################################## PARAMETRES DE CONNEXIÓ ###################################

Host = 'dcccluster.uab.es' 
Port = 8201

###################################### CONNEXIÓ ##############################################

DSN = "mongodb://{}:{}".format(Host,Port)
conn = MongoClient(DSN)

############################# TRANSFERÈNCIA DE DADES AMB MONGO ##############################

#creació de les col·leccions solament si no s'han creat anteriorment
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

#tractament dels paràmetres d'entrada, per esborrar o carregar els arxius
parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file', type=str)
parser.add_argument('-delete_all','--bd',type=str)
args = parser.parse_args()


if args.bd==None:
    #si no s'ha seleccionat l'esborrament de les dades, es carregaran les dades dels arxius
    try:
        #un diccionari o llista per cada col·lecció, que depèn de com es tractin les seves dades
        coleccions={}
        editorials={}
        publicacions=[]
        artistes=[]
        personatges={}
        
        #afegim les ids de les dades que hi havia a les col·leccions amb anterioritat, per així no afegir duplicats
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
        
        #llegim el arxiu d'excel Colleccions-Publicacions
        if args.file is not None:
            excel_col=pd.read_excel(args.file,sheet_name='Colleccions-Publicacions',header=0)
        excel_def=list(excel_col.columns)
        data_col = []
        for i, row in excel_col.iterrows():
            dict_row = {}
            for j, value in enumerate(row):
                dict_row[excel_def[j]] = value
            data_col.append(dict_row)
        
        #iterem sobre les dades trobades a cada línea de l'Excel, que estan en forma de diccionari
        for publicacio in data_col:
            #com que el nom de la editorial es la id, doncs comprovem que el nom de la editorial no estigui ja a la llista de les ids afegides a la col·lecció editorials
            if publicacio['NomEditorial'] not in lista_ids_edit:
                editorial_prov={}
                editorial_prov['_id']=publicacio['NomEditorial']
                editorial_prov['responsable']=publicacio['resposable']
                editorial_prov['adreca']=publicacio['adreca']
                editorial_prov['pais']=publicacio['pais']
                #editorial_prov['coleccions']=[publicacio['NomColleccio']]
                editorials[publicacio['NomEditorial']]=editorial_prov
            
            #com que el nom de la col·lecció es la id, doncs comprovem que el nom de la col·lecció no estigui ja a la llista de les ids afegides a la col·lecció col·leccions
            if publicacio['NomColleccio'] not in lista_ids_col:
                coleccio_prov={}
                #només afegim les col·leccions noves, ja que poden aparèixer varies vegades
                if publicacio['NomColleccio'] not in coleccions:
                    coleccio_prov['_id']=publicacio['NomColleccio']
                    coleccio_prov['total_exemplars']=publicacio['total_exemplars']
                    coleccio_prov['genere']=publicacio['genere'][1:len(publicacio['genere'])-1].split(', ')
                    coleccio_prov['idioma']=publicacio['idioma']
                    coleccio_prov['any_inici']=publicacio['any_inici']
                    coleccio_prov['any_fi']=publicacio['any_fi']
                    coleccio_prov['tancada']=publicacio['tancada']
                    coleccio_prov['editorials']=[publicacio['NomEditorial']]
                    coleccions[publicacio['NomColleccio']]=coleccio_prov
                else:
                    #tot i que una col·lecció ja hagi estat afegida, la línea pot contenir editorials noves
                    if publicacio['NomEditorial'] not in coleccions[publicacio['NomColleccio']]['editorials']:
                        coleccions[publicacio['NomColleccio']]['editorials'].append(publicacio['NomEditorial'])
            
            #com que el nom de la publicació es la id, doncs comprovem que el nom de la publicació no estigui ja a la llista de les ids afegides a la col·lecció publicacions
            if publicacio['ISBN'] not in lista_ids_publi:
                publicacio_prov={}
                publicacio_prov['_id']=publicacio['ISBN']
                publicacio_prov['titol']=publicacio['titol']
                publicacio_prov['stock']=publicacio['stock']
                publicacio_prov['autor']=publicacio['autor']
                publicacio_prov['preu']=publicacio['preu']
                publicacio_prov['num_pagines']=publicacio['num_pagines']
                publicacio_prov['guionistes']=publicacio['guionistes'][1:len(publicacio['guionistes'])-1].split(', ')
                publicacio_prov['dibuixants']=publicacio['dibuixants'][1:len(publicacio['dibuixants'])-1].split(', ')
                publicacio_prov['id_coleccio']=publicacio['NomColleccio']
                publicacio_prov['editorial']=publicacio['NomEditorial']
                publicacions.append(publicacio_prov)
        
        #llegim el arxiu d'excel Personatges
        if args.file is not None:
            excel_pers=pd.read_excel(args.file,sheet_name='Personatges',header=0)
        excel_def=list(excel_pers.columns)
        data_pers = []
        for i, row in excel_pers.iterrows():
            dict_row = {}
            for j, value in enumerate(row):
                dict_row[excel_def[j]] = value
            data_pers.append(dict_row)
        
        #iterem sobre les dades trobades a cada línea de l'Excel, que estan en forma de diccionari
        for persona in data_pers:
            #com que el nom dels personatges són la id, doncs comprovem que el nom del personatge no estigui ja a la llista de les ids afegides a la col·lecció personatges
            if persona['nom'] not in lista_ids_pers:
                persona_prov={}
                #un personatge pot aparèixer varies vegades, però només l'afegirem una
                if persona['nom'] not in personatges:
                    persona_prov['_id']=persona['nom']
                    persona_prov['tipus']=persona['tipus']
                    persona_prov['isbn']=[persona['isbn']]
                    personatges[persona['nom']]=persona_prov
                else:
                    #tot i que un personatge ja hagi estat afegir, pot ser a un altre llibre, y llavors afegim el seu isbn
                    personatges[persona['nom']]['isbn'].append(persona['isbn'])
        
        #llegim el arxiu d'excel Artistes
        if args.file is not None:
            excel_artist=pd.read_excel(args.file,sheet_name='Artistes',header=0)
        excel_def=list(excel_artist.columns)
        data_artist = []
        for i, row in excel_artist.iterrows():
            dict_row = {}
            for j, value in enumerate(row):
                dict_row[excel_def[j]] = value
            data_artist.append(dict_row)
        
        #iterem sobre les dades trobades a cada línea de l'Excel, que estan en forma de diccionari
        for artista in data_artist:
            #com que el nom del artista és la id, doncs comprovem que el nom del artista no estigui ja a la llista de les ids afegides a la col·lecció artistes
            if artista['Nom_artistic'] not in lista_ids_artist:
                artista_prov={}
                artista_prov['_id']=artista['Nom_artistic']
                artista_prov['nom']=artista['nom']
                artista_prov['cognoms']=artista['cognoms']
                artista_prov['data_naix']=artista['data_naix'].to_pydatetime().isoformat()
                artista_prov['pais']=artista['pais']
                artistes.append(artista_prov)
        
        #les dades que s'havien guardat en un diccionari de diccionaris s'afegueixen a una llista per poder utilitzar la funció insert_many
        coleccions_list=[]
        for key in coleccions:
            coleccions_list.append(coleccions[key])
        editorials_list=[]
        for key in editorials:
            editorials_list.append(editorials[key])
        personatges_list=[]
        for key in personatges:
            personatges_list.append(personatges[key])
        
        #s'afegeixen les noves dades a les col·leccions mitjançant insert_many
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
    #si s'ha seleccionat esborrar la base de dades, s'itera sobre totes les coleccions i s'esborren una a una
        collection_list = bd.list_collection_names()
        for collection in collection_list:
            bd[collection].drop()

conn.close()
    

        