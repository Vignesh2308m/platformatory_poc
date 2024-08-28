### Produce synthetic purchase messages to kafka ###
import io
from confluent_kafka import Producer
import socket
import random as rd
import json
import os
import avro.io
import avro.schema
from fastavro.utils import generate_many
from datetime import datetime

def init_cloud():
    conf = {'bootstrap.servers': os.getenv("BOOTSTRAPSERVER"),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': os.getenv("USERNAME"),
        'sasl.password': os.getenv("PASSWORD"),
        'client.id': socket.gethostname()}

    return Producer(conf)

def init_local():
    conf = {'bootstrap.servers': os.getenv("BOOTSTRAPSERVER"),
            'client.id': socket.gethostname()}

    return Producer(conf)

def serialize_avro(record, schema):
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer = avro.io.DatumWriter(schema)
    writer.write(record, encoder)
    return bytes_writer.getvalue()

def ack(err,msg):
    if err is not None:
        print(f" Unable to sent message due to \n {str(err)}, \n Message:{str(msg)}")
    else:
        print(f" Message sent!\n{str(msg)}")

def main():
    env=os.getenv("ENV")

    if env=="cloud":
        producer=init_cloud()
    elif env=="local":
        producer=init_local()
    else:
        print("Not a valid enviroment")
        return None

    topic=os.getenv("TOPIC")

    with open("purchase_schema.json") as f:
        schema=json.load(f)

    avro_schema=avro.schema.parse(str(schema).replace("'","\""))
    
    while True:
        for i in generate_many(schema,1000):
            x=serialize_avro(i,avro_schema)

            # Create a timestamp
            timestamp_ntz = datetime.now()

            # Cast the timestamp to a string
            timestamp_str = timestamp_ntz.strftime("%Y-%m-%d %H:%M:%S")

            # Encode the string as bytes for Kafka
            kafka_key = timestamp_str.encode('utf-8')

            producer.produce(topic,key=kafka_key,value=x,callback=ack)

            producer.poll(1)

if __name__=="__main__":
    main()
