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

            # Send the messages to the 'world_clock_topic' Kafka topic
            producer.send(kafka_topic, current_datetime_message.encode('utf-8'))

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
    clock_frame = tk.Frame(window, width=800, height=500)
    clock_frame.pack(fill=tk.BOTH, expand=True)

    blank_row_label_1 = tk.Label(clock_frame, text=None)
    blank_row_label_1.grid(row=0, column=0)

    blank_row_label_2 = tk.Label(clock_frame, text=None)
    blank_row_label_2.grid(row=1, column=0)

    city_labels = {}
    time_labels = {}

    for row, city in enumerate(cities.keys()):
        city_label = tk.Label(clock_frame, text=None, bg="lightblue", fg="white", font=("Courier", 20))
        city_label.grid(row=row+2, column=0, padx=5, pady=5)
        city_labels[city] = city_label
        
        
        timezone_label = tk.Label(clock_frame, text=None, font=("Courier", 20))
        timezone_label.grid(row=row+2, column=1, sticky='w', padx=5, pady=5)
        time_labels[city] = timezone_label



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


            # Check if this is a new message for the city
            if city not in latest_messages or latest_messages[city] !=  message_value:

                # Update the latest message for this city 
                latest_messages[city] = message_value


                # Update the time displayed for each city
                time_labels[city]['text'] = f"{city}:                 {time_str}            {date_str}"

                # print(time_labels[city]['text'])



    # Display the messages in the Tkinter UI via threads
    consumer_thread = threading.Thread(target=get_messages)
    consumer_thread.start()

    window.mainloop()




if __name__ == '__main__':

    # Run the operations concurrently 
    producer_thread = threading.Thread(target=create_producer)
    producer_thread.start()

create_world_clock_ui()
