import uuid
import random
import datetime
import json
import time
from confluent_kafka import Producer

# Kafka configuration for Confluent Cloud
#Ce dictionnaire contient les paramètres de connexion à notre broker Kafka
producer_config = {
    'bootstrap.servers': 'pkc-e0zxq.eu-west-3.aws.confluent.cloud:9092', #L’adresse du broker Kafka (ou de Confluent Cloud)
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'DQQ5OL233HIR6DEL',      
    'sasl.password': 'Afnhs2tBaPGYTNiTa7g6TZzB6j+lTDY6qhst4niFQHsKCZD8yI5M3BnSYdKkiq26'    
}

producer = Producer(producer_config)


base_time = datetime.datetime(2023, 10, 27, 3, 53, 49)


def generate_timestamp(previous_time):
    #"""Generate the next timestamp with random increments in seconds."""
    increment = random.randint(1, 120)
    return previous_time + datetime.timedelta(seconds=increment)

def generate_log_entry(previous_time):
    #"""Generate a single log entry with the required fields."""
    timestamp = generate_timestamp(previous_time)
    order_id = str(uuid.uuid4())
    order_product_name=random.choice(["iphone11", "iphone12", "", "iphone13", "iphone14", "iphone15","iphone16"])
    order_card_type=random.choice(["Visa", "MasterCard", "American_Express"])
    order_amount=round(random.uniform(10.0, 1000.0), 2)
    order_datetime=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    order_country_name=random.choice(["France", "Italie", "Espagne", "Maroc", "USA"])
    match order_country_name:
        case "France":
            order_city_name = random.choice(["Paris", "Strasbourg"])
        case "Italie":
            order_city_name = random.choice(["Rome", "Milan"])
        case "Espagne":
            order_city_name = random.choice(["Madrid", "Bilbao"])
        case "Maroc":
            order_city_name = random.choice(["Casablanca", "Marakech"])
        case "USA":
            order_city_name = random.choice(["NYC", "LA"])
    order_ecommerce_website_name="www.appelPhone2024.com"
    





    # Retourner l'entrée de log sous forme de dictionnaire
    log_entry = {
        "order_id": order_id,
        "product_name": order_product_name,
        "card_type": order_card_type,
        "order_amount": order_amount,
        "order_datetime": order_datetime,
        "order_country": order_country_name,
        "order_city": order_city_name,
        "ecommerce_website": order_ecommerce_website_name,
    }
    return log_entry, timestamp


def produce_log_stream(topic, num_entries=10):
    current_time = base_time
    for _ in range(num_entries):
        log_entry, current_time = generate_log_entry(current_time)
        # Convert to string format and produce to Kafka topic
        #La méthode produce envoie l’entrée de log au sujet Kafka (topic).
        producer.produce(topic, key=log_entry["order_id"], value=str(log_entry))
        #value=str(log_entry) : Le dictionnaire d'entrée de log est converti en chaîne de caractères avant d'être envoyé.
        
        producer.flush()  # Ensure message is sent immediately
        print(f"Produced: {log_entry}")
        time.sleep(1)  # Delay to simulate streaming data

produce_log_stream("log_web", 10)
