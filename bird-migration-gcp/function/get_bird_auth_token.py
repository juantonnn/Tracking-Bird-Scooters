# -*- coding: utf-8 -*-
"""
Created on Wed Oct 30 11:59:07 2019

@author: juan.d.marin
"""

import requests
import uuid
import json

def get_auth_token(guid):
    """Fetches auth token from Bird API.
    
    Retrieves an auth token from Bird API by spoofing
    a user login from an iOS device. Uses unique email
    to generate new auth token. Token expires periodically,
    so rerun as necessary.
    
    Args:
        guid: A random 16 Byte GUID of the form
            123E4567-E89B-12D3-A456-426655440000.
    
    Returns:
        A string of the auth token that will be used to
        make future requests to the Bird API.
        
        Returns None if request did not succeed.
    """
    
    url = 'https://api.birdapp.com/user/login'
    headers = {
        'User-Agent': 'Bird/4.41.0 (co.bird.Ride; build:37; iOS 12.3.1) Alamofire/4.41.0',
        'Device-id': guid,
        'Platform':'ios',
        'App-Version': '4.41.0',
        'Content-Type':'application/json',
    }
    
    # Reusing guid to ensure uniqueness
    # Only 1 token permitted per unique email address
    payload = {
        'email':'email@company.com'.format(guid), #Use email that has not been prev registered with Bird App
    }
    
    r = requests.post(url=url, data=json.dumps(payload), headers=headers)
    
    token = r.json().get('token')
    
    return token

# Get auth token before fetching locations
guid = str(uuid.uuid1())
token = get_auth_token(guid)
assert token is not None
print(token)
