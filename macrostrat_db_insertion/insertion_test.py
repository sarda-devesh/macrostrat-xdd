import requests
import json
import os
import time

def make_request():
    request_url = "http://127.0.0.1:9543/record_run"
    with open("example_request.json", "r") as reader:
        request_data = json.load(reader)
    
    # Make the request
    response = requests.post(url = request_url, json = request_data)
    print("Got response of", response.json())
        
if __name__ == "__main__":
    make_request()