import os  # Import the os module for accessing environment variables
import random  # Import the random module for generating random data
import time  # Import the time module for adding delays
import uuid  # Import the uuid module for generating UUIDs

from confluent_kafka import SerializingProducer  # Import the SerializingProducer from confluent_kafka library
import simplejson as json  # Import the json module for JSON serialization
from datetime import datetime, timedelta  # Import datetime module for handling timestamps

# Define coordinates for London and Birmingham
LONDON_COORDINATES = {"latitude": 51.5074, "longitude": -0.1278}
BIRMINGHAM_COORDINATES = {"latitude": 52.4862, "longitude": -1.8904}

# Calculate movement increments based on coordinates
LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['latitude'] - LONDON_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['longitude'] - LONDON_COORDINATES['longitude']) / 100

# Set environment variables for Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')

# Set seed for random number generation
random.seed(42)
# Initialize start time and location for the journey
start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()


def get_next_time():
    """Generate the next timestamp."""
    global start_time
    # Update start time with a random interval between 30 and 60 seconds
    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time


def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    # Generate GPS data with random speed and fixed direction
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(0, 40),
        'direction': 'North-East',
        'vehicleType': vehicle_type
    }


def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    # Generate traffic camera data with random snapshot and location
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'cameraId': camera_id,
        'location': location,
        'timestamp': timestamp,
        'snapshot': 'Base64EncodedString'
    }


def generate_weather_data(device_id, timestamp, location):
    # Generate weather data with random temperature, condition, and other parameters
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'location': location,
        'timestamp': timestamp,
        'temperature': random.uniform(-5, 26),
        'weatherCondition': random.choice(['Sunny', 'Cloudy', 'Rain', 'Snow']),
        'precipitation': random.uniform(0, 25),
        'windSpeed': random.uniform(0, 100),
        'humidity': random.randint(0, 100),
        'airQualityIndex': random.uniform(0, 500)
    }


def generate_emergency_incident_data(device_id, timestamp, location):
    # Generate emergency incident data with random type, status, and description
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'incidentId': uuid.uuid4(),
        'type': random.choice(['Accident', 'Fire', 'Medical', 'Police', 'None']),
        'timestamp': timestamp,
        'location': location,
        'status': random.choice(['Active', 'Resolved']),
        'description': 'Description of the incident'
    }


def simulate_vehicle_movement():
    global start_location
    # Simulate vehicle movement towards Birmingham with some randomness
    start_location['latitude'] += LATITUDE_INCREMENT + random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += LONGITUDE_INCREMENT + random.uniform(-0.0005, 0.0005)
    return start_location


def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()
    # Generate vehicle data including location, speed, and vehicle details
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(10, 40),
        'direction': 'North-East',
        'make': 'BMW',
        'model': 'C500',
        'year': 2024,
        'fuelType': 'Hybrid'
    }


def json_serializer(obj):
    # Custom JSON serializer for handling UUID objects
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')


def delivery_report(err, msg):
    """ Function to handle delivery report of Kafka messages """
    if err is None:
        # If successful(not error), print the message delivered information
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]\n")
    else:
        # If there's an error, print the message delivery failure
        print(f'Message delivery failed: {err}\n')


def produce_data_to_kafka(producer, topic, data):
    """ Produce data to Kafka topic using the provided producer """
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )
    producer.flush()


def simulate_journey(producer, device_id):
    """ Simulate the journey of the vehicle by continuously generating data """
    while True:
        # Generate data for different aspects of the journey
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'],
                                                           vehicle_data['location'], 'Nikon-Cam123')
        weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(device_id, vehicle_data['timestamp'],
                                                                   vehicle_data['location'])

        # Check if the vehicle has reached Birmingham, end simulation if true
        if (vehicle_data['location'][0] >= BIRMINGHAM_COORDINATES['latitude']
                and vehicle_data['location'][1] <= BIRMINGHAM_COORDINATES['longitude']):
            print('Vehicle has reached Birmingham. Simulation ending...')
            break

        # Produce generated data to respective Kafka topics
        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)

        # Add delay to simulate real-time scenario
        time.sleep(3)


if __name__ == "__main__":
    # Configure Kafka producer
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'\nKafka error occurred: {err}\n')
    }
    # Create Kafka producer instance
    serializing_producer = SerializingProducer(producer_config)

    try:
        # Start simulating journey with a specific device ID
        simulate_journey(serializing_producer, 'Vehicle-FXEY110120')

    except KeyboardInterrupt:
        # Handle keyboard interrupt to end simulation gracefully
        print('Simulation ended by the user')
    except Exception as e:
        # Handle unexpected errors during simulation
        print(f'Unexpected Error occurred: {e}')
