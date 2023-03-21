from datetime import datetime
import pytz
import tkinter as tz
from kafka import KafkaProducer, KafkaConsumer
import json
import time



# all_timezones = pytz.all_timezones



# # Display all timezones 

# # timezone_counter = 0
# # for timezone in all_timezones:
# #     timezone_counter += 1
# #     print(f"{timezone_counter}: {timezone}")





bootstrap_servers   =   'localhost:9092'
kafka_topic         =   'world_clock_topic'


cities = {
    'New York': 'America/New York',
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

# time_zone = 'America/New York'
# time_zone = 'Europe/London'
# time_zone = 'Europe/Paris'
# time_zone = 'Africa/Lagos'
# time_zone = 'Africa/Lusaka'
# time_zone = 'Asia/Shanghai'
# time_zone = 'Europe/Madrid'

# now = datetime.now(pytz.timezone(time_zone))

# print(f'Current time: {now} ')

def create_topic():
    """ Create the topic if it doesn't exists. Delete it first if it does exist. """

    pass



def create_producer():

    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

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
            producer.send(kafka_topic, current_datetime_message.encode('utf-8'))

        line_break_message_1 = '---------------------'
        line_break_message_2 = ' '
        line_break_message_3 = '---------------------'
        
        producer.send(kafka_topic, line_break_message_1.encode('utf-8'))
        producer.send(kafka_topic, line_break_message_2.encode('utf-8'))
        producer.send(kafka_topic, line_break_message_3.encode('utf-8'))
        time.sleep(1)
        






def create_consumer():

    consumer = KafkaConsumer(kafka_topic,bootstrap_servers)

    return consumer



def create_world_clock_ui():
    pass




if __name__ == '__main__':
    create_producer()
    create_consumer()