from flask import Flask
from flask import request
import requests
import os
import binascii
import argparse

app = Flask(__name__)
#account = ""
#password = ""
#creds = ("", "")


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
    json = "{'db name':%s,'key':%s,'password':%s}" % \
        (db_name, key.json()["key"], key.json()["password"])
    return json


# Returns an api key given the link to some database
@app.route('/get_key', methods=['PUT'])
def get_key():
    # Check if db name is valid
    r = requests.get("https://" + account + ".cloudant.com/" +
                     request.form['db'], auth=creds)
    if r.status_code == 200:
        key = gen_key(request.form['db'])
        return key.text
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
    return api_key


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
