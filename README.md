# Platformatory POC

Data pipeline to stream and data from Apache Kafka and Postgresdb, process it using Apache Spark and ingest to Apache Kafka topic.

## Objective

- Product Purchase events will be stored in Apache kafka topic called purchase-topic.
- Product details was stored in Postgres Database in the table called public.product
- Need to process data using Apache spark and implement below logic,
    - Every 5 mins, Last 1 hour data should be processed.
    - Need to calculate total price with discount for every purchase.
    - Need to calculate total purchase amount for each products.

## Pipeline Architecture

![alt text]('https://github.com/Vignesh2308m/platformatory_poc/blob/main/imgs/pipeline.png')

## Commands

To run docker compose file,

    `docker compose up --scale spark-worker=2` or
    `docker-compose up --scale spark-worker=2`

To run spark job to write into kafka,

    `docker compose exec spark python3 transform.py` or
    `docker-compose exec spark python3 transform.py` 

To run spark job to see console output,

    `docker compose exec spark python3 transform_console.py` or
    `docker compose exec spark python3 transform_console.py` 

## Results
### Messages in both input and output topics,

![alt text]('https://github.com/Vignesh2308m/platformatory_poc/blob/main/imgs/kafka-topic-both.png')

### Messages in purchase-topic(source) in Avro format,

![alt text]('https://github.com/Vignesh2308m/platformatory_poc/blob/main/imgs/kafka-topic-in.png')

### Messages in top-product-topic(sink) in Avro format,

![alt text]('https://github.com/Vignesh2308m/platformatory_poc/blob/main/imgs/kafka-topic-out.png')

### Product dim table in console mode(spark),
![alt text]('https://github.com/Vignesh2308m/platformatory_poc/blob/main/imgs/product_tbl.png')

### Total purchase per product in every 5 mins for last 1 hour console mode(spark),
![alt text]('https://github.com/Vignesh2308m/platformatory_poc/blob/main/imgs/out.png')
