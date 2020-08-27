

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: paulomatos
"""

# Working directory
import os

path= "/Users/paulomatos/Dropbox/Residential_Voting&Gangs/Analysis/data"
os.chdir(path)


# Mainly spatial and google maps packages 

import pandas as pd
import googlemaps
import polyline 
import geopandas as gpd
from shapely.geometry import Point, LineString
import matplotlib.pyplot as plt

api_key = 'AIzaSyCkLRgHTfxHx1AvlSUxvXPs8vFtRsJSmVo' # your API key

gmaps = googlemaps.Client(api_key)

#############################################################################
#############################################################################

# INPUTS

# Neeed shapefiles


# This shapefile is in UTM coordinates, I need to change this 
slv = gpd.read_file("pandillas/guibor maps/" + 
                    "2_Base_de_datos_(Municpios_Pandillas_DistritosElectorales_CENSO2005).shp")


slv = slv.to_crs({'init' :'epsg:4326'}) # transform from UTM to coords 

# Input database


input_df = pd.read_excel("Rutas/rutas.xlsx")


# Before starting the loop I am going to create a empty database to 
# store the results 

output = pd.DataFrame(data = [], columns = ['circuito','latlon0',
                           'latlon1', 'time', 'dist', 'codmun'])

#############################################################################
#############################################################################

# LOOP


# Here starts the loop

for i in range(1, len(input_df)): # loop for each route, there are 40 routes
    
    
    # Intermediate database - only one row in each interaction
    
    interim = pd.DataFrame(data = [], columns = ['circuito','latlon0',
                           'latlon1', 'time', 'dist', 'codmun'])
    
    # The same inputs for the index = i
    
    interim[['circuito','latlon0',
             'latlon1']] = input_df[['circuito','latlon0', 'latlon1']].loc[[i]]  
        
    # geopath point - lat, long
    
    # remember interim and input_df has the same index, so no problem
    
    start = (interim['latlon0'][i]) # string 
    start = start.split(', ')
    
    end =  (interim['latlon1'][i])
    end = end.split(', ')
    
    results = gmaps.directions(start, end, mode = 'driving') # this is the 
            # results we get from googlemaps
    
    
    # get distance and time 
        
    interim['time'] = results[0]['legs'][0]['duration']['value'] # in seconds
    interim['time'] = interim['time']/60 # in minutes 
    
    
    interim['dist'] = results[0]['legs'][0]['distance']['value'] # in meters
    
    # Now, I would like to take the 
    
    route = results[0]['overview_polyline']['points'] # decode route 
    
    lat_lon = polyline.decode(route) # this gave me (lat, lon)
    
    # I need to change this now: lon, lat and convert to spatial object 
    
    lon_lat = [(lon, lat) for lat, lon in lat_lon]
    
    # Now I am going to convert the points and line into spatial objects 
    
    # coords, but first change order 
    
    start = Point(float(start[1]), float(start[0])) # lon, lat
    end = Point(float(end[1]), float(end[0])) # lon, lat
    
    points = gpd.GeoSeries([start, end])
    
    # Here I need to converte the line to a geodataframe to make the 
    # merge with attributes 
    
    route = gpd.GeoDataFrame(interim)
    route['geometry'] = gpd.GeoSeries(LineString(lon_lat))[0] ## adding 
        ## geometry
    
    
    route.crs = {'init' :'epsg:4326'} # define coords    

    # Match route with attributes of the municipality     
    
    route_mun = gpd.sjoin(route, slv, how="inner", op='intersects') 
    route_mun['codmun'] = route_mun['COD_MUN4']
    
    # Then replace the interim database. Note that there could be more than 
    # one match
    
    interim = route_mun[['circuito','latlon0',
                           'latlon1', 'time', 'dist', 'codmun']] # mun identifier 
    
    # Append to the main dataframe 
    
    output = output.append(interim)

    print(i)

   
        
# Finally, after all the records are done, I export the dataset to 
# excel 


output.to_excel("Rutas/rutas_geo.xlsx")

# Checkings # graphs 

# Final graph to check 

# base = slv.plot(edgecolor = 'gray', color = 'None', figsize = (7, 8))
# base = route.plot(ax = base, color = 'red')
# base = gpd.GeoSeries(end).plot(ax = base, marker = 'o', color = 'blue')
# gpd.GeoSeries(start).plot(ax = base, marker = 'o', color = 'green')

#plt.savefig('Analysis/202001_newanalysis/paths/example_map/example_map_1.pdf')