### Produce synthetic purchase messages to kafka ###
import io
from confluent_kafka import Producer
import socket
import random as rd
import json
import avro.io
import avro.schema
from fastavro.utils import generate_many


def init_cloud():
    conf = {'bootstrap.servers': 'pkc-abcd85.us-west-2.aws.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': '<CLUSTER_API_KEY>',
        'sasl.password': '<CLUSTER_API_SECRET>',
        'client.id': socket.gethostname()}

    return Producer(conf)

def init():
    conf = {'bootstrap.servers': 'localhost:9092',
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
    topic="my_topic"
    with open("/home/vickynila/Projects/platformatory_poc/app/source/purchase_producer/purchase_schema.json") as f:
        schema=json.load(f)

    producer=init()

    avro_schema=avro.schema.parse(str(schema).replace("'","\""))
    
    for i in generate_many(schema,1000):
        x=serialize_avro(i,avro_schema)

        producer.produce(topic,key="",value=x,callback=ack)

        producer.poll(1)
    


if __name__=="__main__":
    main()
