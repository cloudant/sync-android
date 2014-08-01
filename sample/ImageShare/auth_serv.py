from flask import Flask
from flask import request
import requests
import os
import binascii
import argparse
import json

app = Flask(__name__)

# Creates a new database and associates it with certain link
@app.route('/')
def create_db():
    # Make sure the name is unique
    while True:
        db_name = "db" + binascii.b2a_hex(os.urandom(10))
        r = requests.put("https://" + account + ".cloudant.com/" + db_name,
                         auth=creds)
        if r.status_code == 201:
            break

    key = gen_key(db_name)
    key["db_name"] = db_name;
    return json.dumps(key)


# Returns an api key given the link to some database
@app.route('/get_key', methods=['PUT'])
def get_key():
    #import pdb;pdb.set_trace()
    # Check if db name is valid
    r = requests.get("https://" + account + ".cloudant.com/" +
                     request.get_json()['db'], auth=creds)
    if r.status_code == 200:
        key = gen_key(request.get_json()['db'])
        return json.dumps(key)
    else:
        return "Invalid db name!"


# Generate an API key and give it permissions
def gen_key(db_name):
    api_key = requests.post("https://cloudant.com/api/generate_api_key",
                            auth=creds)
    requests.post("https://cloudant.com/api/set_permissions",
                  data={
                      "database": account + "/" + db_name,
                      "username": api_key.json()["key"],
                      "roles": ["_reader", "_writer"]
                  },
                  auth=creds)
    json_data = json.loads(api_key.text)
    json_data['login'] = args.login
    return json_data


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Server script for cloudant image sharing sample.')
    parser.add_argument('login')
    parser.add_argument('password')
    args = parser.parse_args()
    global account
    account = args.login
    global password
    password = args.password
    global creds
    creds = (account, password)
    app.run(debug=True)
