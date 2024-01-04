from flask import Flask
from flask_cors import CORS
from flask import request
from kafka import KafkaProducer
from datetime import datetime
import json
import time
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash
import logging

logging.basicConfig(filename="./logs/injector.log",
                            filemode='a',
                            format='%(asctime)s - %(levelname)s : %(message)s',
                            level=logging.INFO)

logging.info("Session Started")

logger = logging.getLogger('injectorlogs')


app = Flask(__name__)
CORS(app)
app.config["DEBUG"] = True


# fill in your kafka brokers and topic name
bootstrap_servers = ""
topic = ""

producer =  KafkaProducer(bootstrap_servers=bootstrap_servers)

# Basic authentication can be used if there is no certificate issued. If no authentication is reuqired all the auth blocks can be ignored
auth = HTTPBasicAuth()
users = {
    "username": generate_password_hash("nzYfCp9CA9uVPjcK")
}

@auth.verify_password
def verify_password(username, password):
    if username in users:
        return check_password_hash(users.get(username), password)
    return False

# this function is for publishing data to kafka topic. Can be played to automatically generate key , topic and value through logic
@app.route('/injector/inject', methods=['POST'])
@auth.login_required
def inject():
    try:
        payload = request.data        
        logger.info(f"Recieved data to be published {payload}")
        producer.send(topic, key = "", value = str(payload).encode()) 
        response = json.dumps({"message": "Inject Succesfull."})                      

        
    except Exception as e:
        print(str(e))
        response = json.dumps({"message": "Inject Failure." + str(e)})

    return response



@app.route('/', methods=['GET'])
def baseping():
    response = json.dumps({"message": "Ping Success."})
    return response

# certificate and key created during csr process
context = ('domain.crt', "domain.key")

app.run(host='0.0.0.0', port  = '443' , ssl_context=context)
