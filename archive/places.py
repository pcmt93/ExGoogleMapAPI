#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug  7 14:45:59 2019

@author: paulomatos

In this script I am going to try get the location of 
all government offices in San Salvador

Update: I change this to get different kind of places 

References: 
    https://andrewpwheeler.wordpress.com/2014/05/15/using-the-google-places-api-in-python/
    https://python.gotrained.com/google-places-api-extracting-location-data-reviews/#more-1293
    
    
Using json and google place API

"""

import os

db = "/Users/paulomatos/Dropbox/Crime and Development (El Salvador)"
path= db + "/Data/government"


os.chdir(path)

import json
import requests
import time
from random import randrange


import pandas as pd 

# Here I am going to define the function that will find 
# all the places, including the lat long and other 
# additional information

def google_places(lat, lng, radius, types, key):
    
    # Defining URL & parameters

    url = ('https://maps.googleapis.com/maps/api/place/nearbysearch/json')

    
    params = {
            
            'location': str(lat) + "," + str(lng),
            'radius': radius,
            'types': types,
            'key': key,
    }
    
    # Grabbing the JSON result
    
    response = requests.get(url, params = params)
    jsonData = json.loads(response.content) # first page 
    
    jsonDataT = [] # Now we are going to include all the 
        # results 
    
    jsonDataT.extend(jsonData['results'])
    
    time.sleep(2)
    while "next_page_token" in jsonData:
        
        params['pagetoken'] = jsonData['next_page_token']
        response = requests.get(url, params = params)
        jsonData = json.loads(response.content)
        jsonDataT.extend(jsonData['results'])
        time.sleep(2)
        
    return jsonDataT

# Now I am making a list for the resulting elements
    
def ListJson(place):
    
    x = [place['place_id'], place['name'], 
         place['geometry']['location']['lat'],
         place['geometry']['location']['lng'],
         ', '.join(place['types']), place['vicinity']]
    
    return x


# Parameters:

MyKey = 'AIzaSyCkLRgHTfxHx1AvlSUxvXPs8vFtRsJSmVo' ## API key created 08/08/19

Types = 'local_government_office' # only supported types, missing if you want to 
    # get all the types 
    # check here: https://developers.google.com/places/supported_types
    

# Reference points 
    
refpoints = pd.read_csv('refpoints.csv', encoding = 'utf8')    


# I am going to do the search for each point, the problem is that there are
# too many!:

temp = []


for index, point in refpoints.iterrows():

    
    time.sleep(randrange(1,2)) # to avoid detection
    
    search = google_places(lat = point['lat'], 
                           lng = point['lng'],
                           radius = 354, types = Types,
                           key = MyKey) 
        
    ## to try to get the most address as
        ## possible I have choosen a very reduced radius
        ## este ratio equivale a grids de 500 mts aprox. luego hay que eliminar
        ## dups 
        
    time.sleep(randrange(1,2)) # to avoid detection
    
    print(str(index) + ' --> ' + str(point['lat']) + ', ' + str(point['lng']))
    
    for place in search:
        
        x = ListJson(place)
        x[:0] = [point['ID']] ## agregamos el ID del grid de referencia
        temp.append(x)
        
    df_temp = pd.DataFrame(temp, columns=['idG', 'id', 'name', 'lat',
                       'lng', 'type', 'reference'])
    
    print(len(df_temp)) ## this is a temp file just in case 
        ## something goes wrong
    
    pd.DataFrame(df_temp).to_csv('gov_back.csv', encoding='utf8')

        
# Now we put everything in a pandas dataframe 

df = df_temp
df = df[df.type.str.contains("local_government_office")] ## just doule check

pd.DataFrame(df).to_csv('gov_dups.csv', encoding='utf8')

# Now I am going to drop duplicates 

df_unique = df.drop_duplicates(['id']) ## dropeamos empresas duplicadas
pd.DataFrame(df_unique).to_csv('gov_unique.csv', encoding='utf8')
    
    
    
