#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 29 19:02:46 2018

@author: paulomatos
"""

# Script for geocoding points from string variables
# It is recommended that you first clean the string address variables
# before you start geocoding (specially if you are using addresses 
# in other language different than English)

import os

# Define your working directory here:
    
db = "/Users/paulomatos/Dropbox/Repos/ExGoogleMapAPI"
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

API_KEY = "" # You need to get an API key from google cloud. There is 
    # one free trial that allows you to geocode around 100k addresses

BACKOFF_TIME = 30 # because now there is no limit per day, just per second 
# rest a minute, que can vary it 

# Output filename here 

output_filename = 'geo_12588_pm.csv'

# Input filename 
1
input_filename = 'direcciones12588_pm.csv'

# Column name that contains address

address_column_name = 'dir1_completa'
id_dataframe = 'codigoempresa'

# Return Full Google results, could be useful

RETURN_FULL_RESULTS = False

#---------------------------------------------------------------#

# Read the data 

slv = pd.read_csv(input_filename, encoding = 'utf8')

if address_column_name not in slv.columns:
    raise ValueError('Missing Address column in input data')
    
addresses = slv[address_column_name] ## only matters the address turn to 
# dataframe to object type 

# dataframe to object type 
id = slv[id_dataframe]


#--------------------------------------------------------------#

# funtion definitions 

def get_google_results(address, api_key = None, return_full_response = False):
    
    """
    This function will help us get the geocode results from Google Maps 
    Geocoding API
    
    This function gets the detail of the first result
    
    It is important that address is as accurate as possible
    As mentioned before api_key = None, only 2500 cases per day
    
    """

    geocode_url = "https://maps.googleapis.com/maps/api/geocode/json?address={}".format(address)

    if api_key is not None:
        
        geocode_url = geocode_url + "&key={}".format(api_key) ## just to keep
        ## it general, if in case we get an api key
    
    # Pingo google for the results
    
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
    
    # Append some other details:
    
    output['id'] = id
    output['input_string'] = address
    output['number_of_results'] = len(results['results'])
    output['status'] = results.get('status')
    
    if return_full_response is True:
        output['response'] = results
    
    return output
    
#------------------------------------------------------------------#
    
# Loop 
    
results = [] # list 

# Go to each address in the list of addresses 

for address in addresses:
    
    # While the address geocoding is not finished:
    
    geocoded = False 
    
    while geocoded is not True:
        
        # Geocode the address with google
        
        try:
            geocode_result = get_google_results(address, API_KEY,
                                                return_full_response=RETURN_FULL_RESULTS)
        except Exception as e:
            logger.exception(e)
            logger.error("Major error with {}".format(address))
            logger.error("Skipping!")
            geocoded = True
            
        # If we're over the API limit, backoff for a while and try again later.
        if geocode_result['status'] == 'OVER_QUERY_LIMIT':
            logger.info("Hit Query Limit! Backing off for a bit.")
            time.sleep(BACKOFF_TIME) # sleep for xx minutes
            geocoded = False
            
        else:
            
            # If we're ok with API use, save the results
            # Note that the results might be empty / non-ok - log this
            if geocode_result['status'] != 'OK':
                logger.warning("Error geocoding {}: {}".format(address, geocode_result['status']))
            logger.debug("Geocoded: {}: {}".format(address, geocode_result['status']))
            results.append(geocode_result)           
            geocoded = True
            
            
    # Print status every 100 addresses 
    
    if len(results) % 100 == 0:
        logger.info("Completed {} of {} address".format(len(results), 
                    len(addresses)))
    
    # Every 500 addresses, save progress to file (just in case :) )
    
    if len(results) % 500 == 0:
        pd.DataFrame(results).to_csv("{}_bak".format(output_filename))
        
# All done 

logger.info("Finished geocoding all addresses")
# Write the full results to csv using the pandas library.

pd.DataFrame(results).to_csv(output_filename, encoding='utf8')


#----------------------------------------------------------------------------
#----------------------------------------------------------------------------


