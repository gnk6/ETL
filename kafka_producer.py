#!/usr/bin/env python3
from neo4j import GraphDatabase
from flask import Flask, request
from pymongo import MongoClient
from kafka import KafkaProducer
from kafka import KafkaConsumer
import mysql.connector
import json

app = Flask(__name__)
uri = "neo4j://localhost:7687"
mongoURI = 'mongodb://localhost:27017'

def neoConn(uid,uri):
    with GraphDatabase.driver(uri, auth=('neo4j','neo4j12345')) as driver:
        with driver.session() as session:
            result = session.run("MATCH (p:Person {uid: $pid})-[:Friend]-(friend) RETURN p.name AS person, p.uid as user_id, p.favoriteband AS bands, collect(friend.uid) AS friends", pid=uid)
            result_list = result.data()
        if len(result_list) == 0:
               return 'error' 
        else:
            return result_list[0]

def mongoConn(mongouri, start_date, end_date):
    mongo_client = MongoClient(mongouri)
    db = mongo_client['bandDB']
    table = db['bands']
    bands = list(table.find({'formation_year': {'$gte': start_date, '$lte': end_date}}))
    return bands

def publish_to_kafka_user(user_data):
    print(user_data)
    producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    producer.send('users-topic',key=b'user', value=user_data)
    producer.flush

def publish_to_kafka_band(band_data):
    print(band_data)
    producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    producer.send('bands-topic', key=b'band', value=band_data)
    producer.flush()

@app.route('/users', methods=['GET'])
def publish_users():
    uid = request.json.get('user')
    user_data = neoConn(uid,uri)
    if user_data=='error':
        return 'User does not exists\n', 400
    for frid in user_data['friends']: ##Publish all friends of the requested user
        friend_data = neoConn(frid,uri)
        del friend_data['friends'] ##This pair is not needed
        publish_to_kafka_user(friend_data)
    del user_data['friends']
    publish_to_kafka_user(user_data)  
    return 'Published users to Kafka\n', 200

@app.route('/bands', methods=['GET'])
def publish_bands():
    start_date = request.json.get('start_date')
    end_date = request.json.get('end_date')
    bands = mongoConn(mongoURI, start_date, end_date)
    for band in bands:
        del band['_id']
        publish_to_kafka_band(band)
    return 'Published bands to Kafka\n', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0',port=5000, debug=True)
