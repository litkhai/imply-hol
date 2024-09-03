import requests
import json
import random
import uuid
import logging
from datetime import datetime, timedelta
import aiohttp
import asyncio

# Import the configuration variables
from config import INGESTION_URL, HEADERS, NUM_RIDERS, CPU_COUNT

# Set up logging
logging.basicConfig(
    filename='data_generator.log',  # Log output file
    level=logging.INFO,                   # Log level (INFO, DEBUG, ERROR)
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Function to generate a random initial location (latitude, longitude)
def generate_initial_coordinates():
    return round(random.uniform(-90, 90), 6), round(random.uniform(-180, 180), 6)

# Function to generate a small random change in coordinates
def generate_coordinate_drift(latitude, longitude):
    # Drift the latitude and longitude slightly to simulate movement
    new_latitude = latitude + random.uniform(-0.0005, 0.0005)
    new_longitude = longitude + random.uniform(-0.0005, 0.0005)
    return round(new_latitude, 6), round(new_longitude, 6)

# Function to generate random speed
def generate_random_speed():
    return round(random.uniform(0, 120), 2)  # Speed in km/h

# Function to generate a random direction
def generate_random_direction():
    return random.randint(0, 360)

# Function to generate a random battery level
def generate_random_battery_level():
    return random.randint(0, 100)

# Function to generate a random network type
def generate_random_network_type():
    return random.choice(["2G", "3G", "4G", "5G", "Wi-Fi"])

# Function to generate a random signal strength
def generate_random_signal_strength():
    return random.randint(-120, -30)  # Signal strength in dBm

# Function to generate a random trip status
def generate_random_trip_status():
    return random.choice(["searching", "en_route", "completed", "canceled"])

# Function to generate a random event type
def generate_random_event_type():
    return random.choice(["trip_started", "trip_ended", "location_update", "battery_low"])

# Function to generate a random app version
def generate_random_app_version():
    return f"{random.randint(1, 5)}.{random.randint(0, 9)}.{random.randint(0, 9)}"

# Function to generate a random timestamp for a specific day
def generate_random_timestamp(day_offset):
    base_time = datetime.now() - timedelta(days=day_offset)
    random_time = base_time.replace(hour=random.randint(0, 23), minute=random.randint(0, 59), second=random.randint(0, 59))
    return random_time.isoformat()

# Function to generate random nested attributes for environment info
def generate_environment_info():
    return {
        "temperature": round(random.uniform(-10, 40), 2),
        "weather_condition": random.choice(["clear", "rainy", "cloudy", "snowy"]),
        "traffic_congestion_level": random.choice(["low", "medium", "high"]),
        "traffic_incident_reported": random.choice([True, False]),
    }

# Function to generate a list of rider profiles
def generate_riders(num_riders=NUM_RIDERS):
    riders = []
    for _ in range(num_riders):
        gender = random.choice(["male", "female", "non-binary"])
        age = random.randint(18, 60)
        experience_year = random.randint(1, 30)
        rider_id = f"R{uuid.uuid4().hex[:8]}"
        riders.append({
            "rider_id": rider_id,
            "gender": gender,
            "age": age,
            "experience_year": experience_year,
            "rider_status": random.choice(["available", "on_trip", "offline"]),
        })
    return riders

# Function to generate a single data record for a specific trip
def generate_data_record(trip_info, day_offset):
    # Update coordinates based on previous coordinates to ensure continuity
    trip_info['current_latitude'], trip_info['current_longitude'] = generate_coordinate_drift(
        trip_info['current_latitude'], trip_info['current_longitude']
    )

    environment_info = generate_environment_info()

    data_record = {
        # Rider info fields at the root level
        "rider_id": trip_info['rider_info']['rider_id'],
        "gender": trip_info['rider_info']['gender'],
        "age": trip_info['rider_info']['age'],
        "experience_year": trip_info['rider_info']['experience_year'],
        "rider_status": trip_info['rider_info']['rider_status'],

        # Nested trip_info fields
        "trip_info": {
            "trip_id": trip_info['trip_id'],
            "trip_status": trip_info['trip_status'],
            "pickup_location": {
                "latitude": trip_info['pickup_latitude'],
                "longitude": trip_info['pickup_longitude']
            },
            "dropoff_location": {
                "latitude": trip_info['dropoff_latitude'],
                "longitude": trip_info['dropoff_longitude']
            },
            "current_trip_distance": trip_info['current_trip_distance'],  # Distance in km
            "estimated_time_of_arrival": trip_info['estimated_time_of_arrival']
        },

        # Nested location_info fields
        "location_info": {
            "current_latitude": trip_info['current_latitude'],
            "current_longitude": trip_info['current_longitude'],
            "speed": generate_random_speed(),
            "direction": generate_random_direction(),
            "accuracy": round(random.uniform(1, 10), 2)
        },

        # Nested device_info fields
        "device_info": {
            "device_id": trip_info['device_id'],
            "battery_level": generate_random_battery_level(),
            "network_type": generate_random_network_type(),
            "signal_strength": generate_random_signal_strength(),
            "app_version": generate_random_app_version()
        },

        # Nested environment_info fields
        "environment_info": environment_info,

        # Event info fields at the root level
        "timestamp": generate_random_timestamp(day_offset),
        "event_type": generate_random_event_type(),
        "event_codes": [random.randint(100, 500) for _ in range(random.randint(0, 2))]
    }

    return data_record

# Function to generate initial trip information for continuity
def generate_initial_trip_info(rider, day_offset):
    pickup_latitude, pickup_longitude = generate_initial_coordinates()
    dropoff_latitude, dropoff_longitude = generate_initial_coordinates()
    trip_id = f"T{uuid.uuid4().hex[:8]}"

    trip_info = {
        "rider_info": rider,
        "trip_id": trip_id,
        "trip_status": generate_random_trip_status(),
        "pickup_latitude": pickup_latitude,
        "pickup_longitude": pickup_longitude,
        "dropoff_latitude": dropoff_latitude,
        "dropoff_longitude": dropoff_longitude,
        "current_trip_distance": round(random.uniform(0, 50), 2),  # Distance in km
        "estimated_time_of_arrival": generate_random_timestamp(day_offset),
        "current_latitude": pickup_latitude,
        "current_longitude": pickup_longitude,
        "device_id": f"D{uuid.uuid4().hex[:8]}",
    }

    return trip_info

# Function to push each record to Imply Polaris via API asynchronously
async def push_to_polaris(session, data_record):
    try:
        async with session.post(INGESTION_URL, headers=HEADERS, json=data_record) as response:
            response_text = await response.text()
            logging.info(f"Response status: {response.status}, Response text: {response_text}")
            response.raise_for_status()
    except Exception as e:
        logging.error(f"Error pushing to Polaris: {e}")

# Function to generate data for a specific day
async def generate_daily_data(riders, day_offset):
    async with aiohttp.ClientSession() as session:
        for _ in range(random.randint(70000, 140000)):  # Records per day
            rider = random.choice(riders)
            trip_info = generate_initial_trip_info(rider, day_offset)
            trip_length = random.randint(5, 20)  # Number of location updates in a trip
            for _ in range(trip_length):
                data_record = generate_data_record(trip_info, day_offset)
                await push_to_polaris(session, data_record)

# Function to generate data for the last 30 days starting from the oldest date
async def generate_30_days_data():
    riders = generate_riders(NUM_RIDERS)  # Generate riders once
    await asyncio.gather(*(generate_daily_data(riders, day_offset) for day_offset in reversed(range(30))))

if __name__ == "__main__":
    logging.info("Starting data generation process.")
    asyncio.run(generate_30_days_data())
    logging.info("Data generation process completed.")

