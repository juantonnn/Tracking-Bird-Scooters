from __future__ import print_function, absolute_import #package to smooth over python 2 and 3 differences

"""
Created on Wed Oct 30 13:26:29 2019
@author: juan.d.marin

This script will request the locations (latitude & longitude)
of Bird Scooters within a set range of a given coordinate.
Several coordinates can be provided at a time.
This data will be uploaded to GCP BigQuery.

"""
from google.cloud import bigquery
from pandas.io.json import json_normalize
import requests
import json
import uuid
import pandas as pd
import datetime as dt

#import os
#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = (
#    'bird-migration-gcp/key/bq_bird_migration_key.json'
#)

#Bird API authorization token
token = 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJBVVRIIiwidXNlcl9pZCI6IjI0YzI2ZWI4LTZm'\
        'NmItNDc2My1iYjkzLWMyMWRhM2FiNmNmOSIsImRldmljZV9pZCI6ImM5ZmFmZjM4LWZiM'\
        'zEtMTFlOS04MGFhLWE0YzNmMGE4ZmUyMCIsImV4cCI6MTYwMzk4ODY4N30.D4EY_3JcXHr'\
        'xxxxxxxxxxxxxxxxxxxxxxxxxx'
guid = str(uuid.uuid1())

######### Accessing BigQuery table #########
client = bigquery.Client()
dataset_id = 'bq_bird_loc' 
table_id = 'birds' 
table_ref = client.dataset(dataset_id).table(table_id)
table = client.get_table(table_ref) 
############################################

def split_location(df, location_label='location'):
    """
    Splits a DataFrame's location column into latitude and longitude columns.
    
    Args:
        df: A DataFrame that contains a column called 'location',
            the elements of which are:
            {
                'latitude': float,
                'longitude': float.
            }.
        location_label: An optional argument to specify the
            label of the location column in df. Default is
            'location' (the label for Bird data).
    
    Returns:
        A DataFrame in which the column with label location_label
        has been removed, and replaced with 'longitude' and 'latitude'
        columns.
    """
    locs = df[location_label].apply(pd.Series)
    df_new = pd.concat([df, locs], axis=1)
    df_new = df_new.drop(location_label, axis=1)
    return df_new

def get_nearby_scooters(token, lat, long):
    """
    Fetches nearby scooters given latitude and longitude.
    
    Args:
        token: Authentication token for Bird API.
        lat: Latitude for center point of search area.
        long: Longitude for center point of search area.
    
    Returns:
        A list of Birds within search area.
        Birds are represented as JSON objects.
    """
    
    url = 'https://api.birdapp.com/bird/nearby'
    params = {
        'latitude':lat,
        'longitude':long,
        'radius':2000
    }

    headers = {
        'Authorization':'Bird {}'.format(token),
        'Device-id':'{}'.format(guid),
        'App-Version':'4.41.0',
        'Location':json.dumps({
            'latitude':lat,
            'longitude':long,
            'altitude':500,
            'accuracy':100,
            'speed':-1,
            'heading':-1
        })
    }

    req = requests.get(url=url, params=params, headers=headers)
    return req.json().get('birds')

def search_multiple_coordinates(token, coordinates):
    """
    Provides data for every scooter 
    located near the provided coordinates.

    Args:
        token: Authentication token for Bird API.
        coordinates: 
            List of latitude & longitude coordinates 
            for search areas.

    Returns: 
        A DataFrame that contains all the API data returned
        from every scooter within the provided coordinate
        search areas, with an added timestamp column.
    """
    bird_df = pd.DataFrame()

    #Aggregate scooter locations found in each coordinate area
    for i in coordinates:
        parsed = get_nearby_scooters(token, i[0], i[1])
        df = split_location(pd.DataFrame.from_records(parsed))
        bird_df = pd.concat([bird_df,df]).drop_duplicates(subset='id').reset_index(drop=True)

    #Add timestamp column
    bird_df.insert(0, 'TimeStamp', pd.datetime.now().replace(microsecond=0))
    
    #Replace NaN values with 0 to prevent any issues with BQ API
    bird_df = bird_df.fillna(0)
    return bird_df

def main():
    coordinates = [
        [33.752072, -84.387572], #Downtown Atlanta
        [33.685823, -84.447422], #East Point 
        [33.786717, -84.384320], #14th & Crescent Ave
        [33.788585, -84.387749], #W Peachtree & 15th
        [33.753275, -84.444466]  #W Lake Ave
    ]
    scooter_df = search_multiple_coordinates(token, coordinates)
    print(scooter_df.head())
    client.insert_rows_from_dataframe(table,scooter_df)
