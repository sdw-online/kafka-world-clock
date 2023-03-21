from datetime import datetime
import pytz
from kafka import KafkaProducer, KafkaConsumer
import json
import time
import threading
import dash
from dash.dependencies import Input, Output
import plotly_express as px
from dash import html 
import dash_core_components as dcc



# Define the constants 
app = dash.Dash(__name__)
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
    main_message_1 = f"Sending messages to '{kafka_topic}' topic " 
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



def get_messages():
    consumer = create_consumer()

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
        if city not in latest_messages or latest_messages[city] != message_value:
            # Update the latest message for this city
            latest_messages[city] = f"{city}:                 {time_str}            {date_str}"

    return latest_messages



app.layout = html.Div([
    html.H1('World Clock by SDW'),
    html.Div(id='clock-frame', children=[
        html.Div([
            html.Div(city, className='city-label', style={'background-color': 'lightblue', 'color': 'white', 'font-family': 'Courier', 'font-size': '20px'}) for city in cities.keys()
        ], className='left-column'),
        html.Div([
            html.Div(id=f"{city}-time", className='time-label', style={'font-family': 'Courier', 'color': 'black','font-size': '20px'}) for city in cities.keys()
        ], className='right-column')
    ]),
    dcc.Interval(id='update-interval', interval=1000),
])


@app.callback(
    [Output(f"{city}-time", "children") for city in cities.keys()],
    Input('update-interval', 'n_intervals')
)
def update_time():
    latest_messages = get_messages()
    return [latest_messages[city] for city in cities.keys()]



if __name__ == '__main__':

    # Run the operations concurrently 
    producer_thread = threading.Thread(target=create_producer)
    producer_thread.start()

    # Start the thread for the consumer 
    consumer_thread = threading.Thread(target=update_time)
    consumer_thread.start()

    # Run the Dash app
    app.run_server(debug=True)
