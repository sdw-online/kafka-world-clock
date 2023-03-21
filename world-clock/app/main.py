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

            # Display the date with the full weekday name, month name and year  
            current_date = now.strftime('%A %d %B %Y')
            # current_date = now.strftime('%Y-%m-%d')
            
            # Structure the message for the producer to send to the Kafka topic  
            current_datetime_message = f" {city}: {current_time} {current_date}"
            basic_line_break_message = ' '

            # Send the messages to the 'world_clock_topic' Kafka topic
            producer.send(kafka_topic, current_datetime_message.encode('utf-8'))
            # producer.send(kafka_topic, basic_line_break_message.encode('utf-8'))


        # Send line breaks to split the messages out in the results
        line_break_message_1 = '---------------------'
        line_break_message_2 = ' '
        line_break_message_3 = '---------------------'
        
        # Send the line breaks to the results
        # producer.send(kafka_topic, line_break_message_1.encode('utf-8'))
        # producer.send(kafka_topic, line_break_message_2.encode('utf-8'))
        # producer.send(kafka_topic, line_break_message_3.encode('utf-8'))

        # Refresh the streams every second
        time.sleep(1)
        



def create_consumer():
    # Consume the latest messages from the Kafka topic
    consumer = KafkaConsumer(kafka_topic,bootstrap_servers=bootstrap_servers)

    return consumer



def create_world_clock_ui():

    # Create the Tkinter window
    window = tk.Tk()
    window.title("World Clock by SDW")
    window.geometry("800x500")


    # Create a frame for all the cities and timezones displayed
    clock_frame = tk.Frame(window)
    clock_frame.pack(fill=tk.BOTH, expand=True)


    city_labels = {}
    time_labels = {}

    for row, city in enumerate(cities.keys()):
        city_label = tk.Label(clock_frame, text=f'{city}:', font=("Helvetica", 16))
        city_label.grid(row=row, column=0, sticky='w', padx=5, pady=5)
        city_labels[city] = city_label
        
        
        timezone_label = tk.Label(clock_frame, text=None, font=('Helvetica', 16))
        timezone_label.grid(row=row, column=1, sticky='e', padx=5, pady=5)
        time_labels[city] = timezone_label


        clock_frame.grid_columnconfigure(10, weight=0, uniform='col')
        clock_frame.grid_columnconfigure(10, weight=1, uniform='col')

    print(city_labels)
    print('')
    print(time_labels)

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


            # Extract the city name and time from each consumed message
            message_extracts = message_value.split(':') 

            city = message_extracts[0].strip()

            # Extract time string 
            time_str = ":".join(message_extracts[1:]).strip()
            time_str = time_str[0:9]

            # Extract date string
            date_str = ":".join(message_extracts[1:]).strip()
            date_str = date_str[9:].strip()

            # print(message_extracts)
            # print(time_str)
            # print(date_str)


            # Check if this is a new message for the city 
            if city not in latest_messages or latest_messages[city] !=  message_value:

                # Update the latest message for this city 
                latest_messages[city] = message_value


                # Update the time displayed for each city
                time_labels[city]['text'] = f" {time_str}    {date_str}"



    # Display the messages in the Tkinter UI via threads
    consumer_thread = threading.Thread(target=get_messages)
    consumer_thread.start()

    window.mainloop()




if __name__ == '__main__':

    # Run the operations concurrently 
    producer_thread = threading.Thread(target=create_producer)
    producer_thread.start()

create_world_clock_ui()
