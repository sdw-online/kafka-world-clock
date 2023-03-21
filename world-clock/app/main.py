from datetime import datetime
import pytz
import tkinter as tk
from kafka import KafkaProducer, KafkaConsumer
import json
import time
import threading

# Define the constants 

bootstrap_servers   =   'localhost:9092'
kafka_topic         =   'world_clock_topic'

cities = {
    'New York': 'America/New_York',
    'London': 'Europe/London',
    'Paris': 'Europe/Paris',
    'Tokyo': 'Asia/Tokyo',
    'Sydney': 'Australia/Sydney',
    'Lagos': 'Africa/Lagos',
    'Lusaka': 'Africa/Lusaka',
    'Shanghai': 'Asia/Shanghai',
    'Madrid': 'Europe/Madrid',
    'Malta': 'Europe/Malta'
}



def create_producer():
    
    # Create the Kafka producer
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    # Create the messages for the producer to send to the Kafka topic
    main_message_1 = f"Sending messages to '{kafka_topic}' topic ... " 
    main_message_2 = f"  " 

    # Send the messages to the Kafka topic
    producer.send(kafka_topic, main_message_1.encode('utf-8'))
    producer.send(kafka_topic, main_message_2.encode('utf-8'))


    # Use an infinite loop to stream the current date and time from different cities and timezones
    while True:
        for city, timezone in cities.items():

            # Get the current date and time for the selected timezone
            now = datetime.now(pytz.timezone(timezone))

            # Parse the date and time to a cleaner format
            current_time = now.strftime('%H:%M:%S')
            current_date = now.strftime('%Y-%m-%d')
            
            # Structure the message for the producer to send to the Kafka topic  
            current_datetime_message = f" {city}: {current_time} {current_date}"
            basic_line_break_message = ' '

            # Send the messages to the 'world_clock_topic' Kafka topic
            producer.send(kafka_topic, current_datetime_message.encode('utf-8'))
            producer.send(kafka_topic, basic_line_break_message.encode('utf-8'))


        # Send line breaks to split the messages out in the results
        line_break_message_1 = '---------------------'
        line_break_message_2 = ' '
        line_break_message_3 = '---------------------'
        
        # Send the line breaks to the results
        producer.send(kafka_topic, line_break_message_1.encode('utf-8'))
        producer.send(kafka_topic, line_break_message_2.encode('utf-8'))
        producer.send(kafka_topic, line_break_message_3.encode('utf-8'))

        # Refresh the streams every second
        time.sleep(1)
        



def create_consumer():
    # Consume the latest messages from the Kafka topic

    consumer = KafkaConsumer(kafka_topic,bootstrap_servers=bootstrap_servers)
    # consumer = KafkaConsumer(kafka_topic,bootstrap_servers=bootstrap_servers, auto_offset_reset='latest')

    return consumer



def create_world_clock_ui():

    # Create the Tkinter window
    window = tk.Tk()
    window.title("World Clock by SDW")
    window.geometry("600x400")

    # Add the digital clock display 
    clock_display = tk.Text(window)
    clock_display.pack(fill=tk.BOTH, expand=True)

    # Set the UI as the consumer of the Kafka messages 
    consumer = create_consumer()

    # Set up the function for displaying the messages in the Tkinter UI
    def get_messages():

        latest_messages = {}
        for message in consumer:
            try:
                message_value = json.loads(message.value.decode('utf-8'))
            except:
                message_value = message.value.decode('utf-8')


            # Extract the city from the consumed message 
            city = message_value.split(':')[0].strip()


            # Check if this is a new message for the city 
            if city not in latest_messages or latest_messages[city] !=  message_value:

                # Update the latest message for this city 
                latest_messages[city] = message_value

                # Only include the latest messages for the city (clear old results)
                clock_display.delete('1.0', tk.END) 

                # Display the latest consumed messages (i.e. city and timezones)
                for city_message in latest_messages.values():
                    clock_display.insert(tk.END, city_message + "\n")

    

    # Display the messages in the Tkinter UI via threads
    consumer_thread = threading.Thread(target=get_messages)
    consumer_thread.start()

    window.mainloop()




if __name__ == '__main__':

    # Run the operations concurrently 
    producer_thread = threading.Thread(target=create_producer)
    producer_thread.start()

    create_world_clock_ui()
