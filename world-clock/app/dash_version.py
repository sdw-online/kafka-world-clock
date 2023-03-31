# Import necessary libraries
from kafka import KafkaProducer
import dash
from dash import html
from dash import dcc
from dash.dependencies import Output, Input
from pytz import timezone
from datetime import datetime



# Define a Kafka producer to send the data to the Kafka topic
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])



# Define a function to get the current time for each city using the pytz and datetime libraries
def get_current_time(city, _timezone):
    now = datetime.now(timezone(_timezone))
    date_str = now.strftime("%A, %d %B %Y")
    # time_str = now.strftime("%I:%M:%S %p")
    time_str = now.strftime("%H:%M:%S")
    return f"{city}, {_timezone}, {time_str}, {date_str}"



# Define a Dash app layout to display the clock
app = dash.Dash(__name__)
app.layout = html.Div([
    html.H1("World Clock"),
    dcc.Interval(
        id='interval-component',
        interval=1000,
        n_intervals=0
    ),
    html.Table([
        html.Thead(html.Tr([
            html.Th("City"),
            html.Th("Time"),
            html.Th("Date")
        ])),
        html.Tbody([
            html.Tr([
                html.Td("New York"),
                html.Td(id="new-york-time"),
                html.Td(id="new-york-date")
            ]),
            html.Tr([
                html.Td("London"),
                html.Td(id="london-time"),
                html.Td(id="london-date")
            ]),
            html.Tr([
                html.Td("Paris"),
                html.Td(id="paris-time"),
                html.Td(id="paris-date")
            ]),
            html.Tr([
                html.Td("Tokyo"),
                html.Td(id="tokyo-time"),
                html.Td(id="tokyo-date")
            ]),
            html.Tr([
                html.Td("Sydney"),
                html.Td(id="sydney-time"),
                html.Td(id="sydney-date")
            ]),
            html.Tr([
                html.Td("Lagos"),
                html.Td(id="lagos-time"),
                html.Td(id="lagos-date")
            ]),
            html.Tr([
                html.Td("Lusaka"),
                html.Td(id="lusaka-time"),
                html.Td(id="lusaka-date")
            ]),
            html.Tr([
                html.Td("Shanghai"),
                html.Td(id="shanghai-time"),
                html.Td(id="shanghai-date")
            ]),
            html.Tr([
                html.Td("Madrid"),
                html.Td(id="madrid-time"),
                html.Td(id="madrid-date")
            ]),
            html.Tr([
                html.Td("Malta"),
                html.Td(id="malta-time"),
                html.Td(id="malta-date")
            ]),
        ])
    ])
])

# Define a callback function to update the clock every second
@app.callback(
    [Output("new-york-time", "children"),   Output("new-york-date", "children"),
     Output("london-time", "children"),     Output("london-date", "children"),
     Output("paris-time", "children"),      Output("paris-date", "children"),
     Output("tokyo-time", "children"),      Output("tokyo-date", "children"),
     Output("sydney-time", "children"),     Output("sydney-date", "children"),
     Output("lagos-time", "children"),      Output("lagos-date", "children"),
     Output("lusaka-time", "children"),     Output("lusaka-date", "children"),
     Output("shanghai-time", "children"),   Output("shanghai-date", "children"),
     Output("madrid-time", "children"),     Output("madrid-date", "children"),
     Output("malta-time", "children"),      Output("malta-date", "children")],
    Input('interval-component', 'n_intervals')
)
def update_clock(n):
    # Get the current time for each city
    new_york_time = get_current_time("New York", "America/New_York")
    london_time = get_current_time("London", "Europe/London")
    paris_time = get_current_time("Paris", "Europe/Paris")
    tokyo_time = get_current_time("Tokyo", "Asia/Tokyo")
    sydney_time = get_current_time("Sydney", "Australia/Sydney")
    lagos_time = get_current_time("Lagos", "Africa/Lagos")
    lusaka_time = get_current_time("Lusaka", "Africa/Lusaka")
    shanghai_time = get_current_time("Shanghai", "Asia/Shanghai")
    madrid_time = get_current_time("Madrid", "Europe/Madrid")
    malta_time = get_current_time("Malta", "Europe/Malta")

    # Send the data to the Kafka topic
    producer.send("world_clock", f"{new_york_time}\n{london_time}\n{paris_time}\n{tokyo_time}\n{sydney_time}\n{lagos_time}\n{lusaka_time}\n{shanghai_time}\n{madrid_time}\n{malta_time}".encode('utf-8'))

    # Return the updated clock values to the app
    return new_york_time.split(", ")[-1],   new_york_time.split(", ")[0], \
           london_time.split(", ")[-1],     london_time.split(", ")[0], \
           paris_time.split(", ")[-1],      paris_time.split(", ")[0], \
           tokyo_time.split(", ")[-1],      tokyo_time.split(", ")[0], \
           sydney_time.split(", ")[-1],     sydney_time.split(", ")[0], \
           lagos_time.split(", ")[-1],      lagos_time.split(", ")[0], \
           lusaka_time.split(", ")[-1],     lusaka_time.split(", ")[0], \
           shanghai_time.split(", ")[-1],   shanghai_time.split(", ")[0], \
           madrid_time.split(", ")[-1],     madrid_time.split(", ")[0], \
           malta_time.split(", ")[-1],      malta_time.split(", ")[0]


app.run_server(debug=True)
