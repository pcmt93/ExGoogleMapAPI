#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 29 19:02:46 2018

Modified: Jan11/2020

@author: paulomatos
"""

# Script for geocoding points from string variables
# It is recommended that you first clean the string address variables
# before you start geocoding (specially if you are using addresses 
# in other language different than English)

import os

# Define your working directory here:
    
db = "/Users/ptrifu/Dropbox/datos-incarceration/data/interim"
os.chdir(db)

# Import needed modules 

import pandas as pd
import requests
import logging
import time

#---------------------------------------------------------------
# configuration 


logger = logging.getLogger("root")
logger.setLevel(logging.DEBUG)

# create console handler  

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

API_KEY = "" 
    # You need to get an API key from google cloud. There is 
    # one free trial that allows you to geocode around 100k addresses

BACKOFF_TIME = 30 # because now there is no limit per day, just per second 
# rest a minute, it is possible to vary it 

# Output filename here 

output_filename = 'prueba_output.xlsx'

# Input filename 

input_filename = 'prueba.xlsx'

# Column name that contains address

address_column_name = 'address'
id_dataframe = 'uid'

# Return Full Google results, could be useful

RETURN_FULL_RESULTS = False

#---------------------------------------------------------------#

# Read the data 

df = pd.read_excel(input_filename)

if address_column_name not in df.columns:
    raise ValueError('Missing Address column in input data')
    

#---------------------------------------------------------------#

# funtion definitions 

def get_google_results(address, api_key = None, return_full_response = False):
    
    """
    This function will help us get the geocode results from Google Maps 
    Geocoding API
    
    This function gets the detail of the first result
    
    It is important that address is as accurate as possible
    As mentioned before api_key = None, only 2500 cases per day
    
    """
    
    # To put in format the key:

    geocode_url = "https://maps.googleapis.com/maps/api/geocode/json?address={}".format(address)

    if api_key is not None:
        
        geocode_url = geocode_url + "&key={}".format(api_key) ## just to keep
        ## it general, if in case we get an api key
    
    # google for the results
    
    results = requests.get(geocode_url)
    
    # Result will be in JSON format - convert to dict using request functionality
    results = results.json()
    
    # if there is no result or an error, return empty results
    
    if len(results['results']) == 0:
        output = {
                
            "formatted_address" : None,
            "latitude": None,
            "longitude": None,
            "accuracy": None,
            "google_place_id": None,
            "type": None,
            "postcode": None
            
        }
        
    else:
        answer = results['results'][0]
        output = {
                
            "formatted_address" : answer.get('formatted_address'),
            "latitude": answer.get('geometry').get('location').get('lat'),
            "longitude": answer.get('geometry').get('location').get('lng'),
            "accuracy": answer.get('geometry').get('location_type'),
            "google_place_id": answer.get("place_id"),
            "type": ",".join(answer.get('types')),
            "postcode": ",".join([x['long_name'] for x in answer.get('address_components') 
                                  if 'postal_code' in x.get('types')])
                
        }


    output['status'] = results.get('status') # to get status of the query
    
    return output
    
#------------------------------------------------------------------#
    
# Loop 
    
results = [] # list 

# Go to each address in the list of addresses 


for ind in df.index:
    
    # While the address geocoding is not finished:
    
    geocoded = False 
    
    while geocoded is not True:
        
        # Geocode the address with google
        
        try:
            geocode_result = get_google_results(df[address_column_name][ind], 
                                                API_KEY, 
                                                return_full_response = 
                                                RETURN_FULL_RESULTS)
        except Exception as e:
            logger.exception(e)
            logger.error("Major error with {}".format(df[address_column_name][ind]))
            logger.error("Skipping!")
            geocoded = True
            
        # If we're over the API limit, backoff for a while and try again later.
        if geocode_result['status'] == 'OVER_QUERY_LIMIT':
            logger.info("Hit Query Limit! Backing off for a bit.")
            time.sleep(BACKOFF_TIME) # Re-try in X seconds
            geocoded = False
            
        else:
            
            # If we're ok with API use, save the results
            # Note that the results might be empty / non-ok - log this
            if geocode_result['status'] != 'OK':
                logger.warning("Error geocoding {}: {}".format(df[address_column_name][ind], 
                                                               geocode_result['status']))
            
            logger.debug("Geocoded: {}: {}".format(df[address_column_name][ind], 
                                                   geocode_result['status']))
            
            # Before appending, I am going to add the columns of the input
            # to the new dataset 
            
            geocode_result[id_dataframe] = df[id_dataframe][ind]
            geocode_result['input_address'] = df[address_column_name][ind]
            
            results.append(geocode_result)           
            geocoded = True
            
            
    # Finally, I am going to append to the output database the 
            
            
    # Print status every 100 addresses 
    
    if len(results) % 100 == 0:
        logger.info("Completed {} of {} address".format(len(results), 
                    len(df[address_column_name])))
    
    # Every 500 addresses, save progress to file (just in case :) )
    
    if len(results) % 500 == 0:
        pd.DataFrame(results).to_csv("{}_bak".format(output_filename))
        
# All done 

logger.info("Finished geocoding all addresses")
# Write the full results to csv using the pandas library.

pd.DataFrame(results).to_excel(output_filename, encoding='utf8')


#----------------------------------------------------------------------------
#----------------------------------------------------------------------------


