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



def create_topic():
    """ Create the topic if it doesn't exists. Delete it first if it does exist. """

    pass



def create_producer():

    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)


    main_message_1 = f"Sending messages to '{kafka_topic}' topic ... " 
    main_message_2 = f"  " 
    producer.send(kafka_topic, main_message_1.encode('utf-8'))
    producer.send(kafka_topic, main_message_2.encode('utf-8'))

    while True:
        for city, timezone in cities.items():
            now = datetime.now(pytz.timezone(timezone))
            current_time = now.strftime('%H:%M:%S')
            current_date = now.strftime('%Y-%m-%d')
            current_datetime_message = f" {city}: {current_time} {current_date}"
            basic_line_break_message = ' '
            producer.send(kafka_topic, current_datetime_message.encode('utf-8'))
            producer.send(kafka_topic, basic_line_break_message.encode('utf-8'))

        line_break_message_1 = '---------------------'
        line_break_message_2 = ' '
        line_break_message_3 = '---------------------'
        
        producer.send(kafka_topic, line_break_message_1.encode('utf-8'))
        producer.send(kafka_topic, line_break_message_2.encode('utf-8'))
        producer.send(kafka_topic, line_break_message_3.encode('utf-8'))
        time.sleep(1)
        



def create_consumer():

    consumer = KafkaConsumer(kafka_topic,bootstrap_servers=bootstrap_servers)

    return consumer



def create_world_clock_ui():

    
    window = tk.Tk()
    window.title("World Clock by SDW")
    window.geometry("600x400")

    clock_display = tk.Text(window)
    clock_display.pack(fill=tk.BOTH, expand=True)


    consumer = create_consumer()

    def get_messages():
        """ Consume the messages from the Kafka topic and display them in the Tkinter UI """


        for message in consumer:
            try:
                message_value = json.loads(message.value.decode('utf-8'))
            except:
                message_value = message.value.decode('utf-8')
            clock_display.insert(tk.END, message_value + "\n")


    consumer_thread = threading.Thread(target=get_messages)
    consumer_thread.start()


    window.mainloop()




if __name__ == '__main__':
    producer_thread = threading.Thread(target=create_producer)
    producer_thread.start()
    create_world_clock_ui()