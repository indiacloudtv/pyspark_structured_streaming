from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import random

# pip install kafka-python

KAFKA_TOPIC_NAME_CONS = "test-topic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                       value_serializer=lambda x: dumps(x).encode('utf-8'))

    product_name_list = ["Laptop", "Desktop Computer", "Mobile Phone", "Wrist Band", "Wrist Watch", "LAN Cable",
                         "HDMI Cable", "TV", "TV Stand", "Text Books", "External Hard Drive", "Pen Drive", "Online Course"]

    order_card_type_list = ["Visa", "MasterCard", "Maestro"]

    country_name_city_name_list = ["Sydney,Australia", "Florida,United States", "New York City,United States",
                                   "Paris,France", "Colombo,Sri Lanka", "Dhaka,Bangladesh", "Islamabad,Pakistan",
                                   "Beijing,China", "Rome,Italy", "Berlin,Germany", "Ottawa,Canada",
                                   "London,United Kingdom", "Jerusalem,Israel", "Bangkok,Thailand",
                                   "Chennai,India", "Bangalore,India", "Mumbai,India", "Pune,India",
                                   "New Delhi,Inida", "Hyderabad,India", "Kolkata,India", "Singapore,Singapore"]

    ecommerce_website_name_list = ["www.datamaking.com", "www.amazon.com", "www.flipkart.com", "www.snapdeal.com", "www.ebay.com"]

    message_list = []
    message = None
    for i in range(500):
        i = i + 1
        message = {}
        print("Preparing message: " + str(i))
        event_datetime = datetime.now()

        message["order_id"] = i
        message["order_product_name"] = random.choice(product_name_list)
        message["order_card_type"] = random.choice(order_card_type_list)
        message["order_amount"] = round(random.uniform(5.5, 555.5), 2)
        message["order_datetime"] = event_datetime.strftime("%Y-%m-%d %H:%M:%S")
        country_name = None
        city_name = None
        country_name_city_name = None
        country_name_city_name = random.choice(country_name_city_name_list)
        country_name = country_name_city_name.split(",")[1]
        city_name = country_name_city_name.split(",")[0]
        message["order_country_name"] = country_name
        message["order_city_name"] = city_name
        message["order_ecommerce_website_name"] = random.choice(ecommerce_website_name_list)

        # order_id,order_product_name,order_card_type,order_amount,order_datetime,order_country_name,order_city_name,order_ecommerce_website_name
        # print("Message Type: ", type(message))
        print("Message: ", message)
        #message_list.append(message)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
        time.sleep(1)

    # print(message_list)

    print("Kafka Producer Application Completed. ")