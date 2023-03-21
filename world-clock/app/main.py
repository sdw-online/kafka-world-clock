from datetime import datetime
import pytz
import tkinter as tz
from kafka import KafkaProducer, KafkaConsumer



all_timezones = pytz.all_timezones



# Display all timezones 

timezone_counter = 0
for timezone in all_timezones:
    timezone_counter += 1
    print(f"{timezone_counter}: {timezone}")