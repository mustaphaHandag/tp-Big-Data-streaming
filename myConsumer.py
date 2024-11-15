from confluent_kafka import Consumer
from collections import defaultdict
import json

# Kafka Consumer configuration
consumer_config = {
    'bootstrap.servers': 'pkc-e0zxq.eu-west-3.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'DQQ5OL233HIR6DEL',
    'sasl.password': 'Afnhs2tBaPGYTNiTa7g6TZzB6j+lTDY6qhst4niFQHsKCZD8yI5M3BnSYdKkiq26',
    'group.id': 'order_analysis_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)
consumer.subscribe(['log_web'])

# Dictionary to store total order amount by country and city
total_order_amount_by_location = defaultdict(float)

try:
    while True:
        # Poll for new messages
        msg = consumer.poll(1.0)
        
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        try:
            # Decode message (ignore any decoding errors)
            message_value = msg.value().decode('utf-8', errors='ignore')
            
            # Fix the single quotes to double quotes to make it valid JSON
            message_value = message_value.replace("'", '"')

            # Now parse the JSON message
            log_entry = json.loads(message_value)
            
            # Extract order amount, country, and city
            order_amount = log_entry.get("order_amount", 0.0)
            order_country = log_entry.get("order_country")
            order_city = log_entry.get("order_city")
            
            # Aggregate total order amount by country and city
            total_order_amount_by_location[(order_country, order_city)] += order_amount

        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            print(f"Invalid message: {msg.value()}")
            continue

except KeyboardInterrupt:
    print("Process interrupted by user")

finally:
    
    consumer.close() 

# Print the total order amount by country and city
print("Total order amount by country and city:")
for (country, city), total_amount in total_order_amount_by_location.items():
    print(f"{country}, {city}: {total_amount}")
