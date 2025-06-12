from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers="kafka:29092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_flight_data():
    return {
        "flight_number": f"AI{random.randint(1000, 9999)}",
        "airline": random.choice(["IndiGo", "Emirates", "Delta", "Qatar Airways"]),
        "route_type": random.choice(["Domestic", "International"]),
       "expected_departure": int(time.time()),  
    "actual_departure": int(time.time() + random.randint(0, 600)) ,
        "capacity": random.randint(100, 400),
        "passengers": random.randint(50, 400),
        "ticket_price_range": f"${random.randint(50, 1000)}",
        "departure_airport": random.choice(["JFK", "LHR", "DXB", "BOM", "SIN"]),
        "arrival_airport": random.choice(["SFO", "SYD", "HKG", "CDG", "YYZ"]),
        "fuel_consumption": random.uniform(5000, 25000),
        "carbon_emission": random.uniform(200, 1000),
        "flight_status": random.choice([ "Cancelled", "Landed"]),
        "luggage_weight": random.uniform(1000, 10000)
    }

while True:
    flight_data = generate_flight_data()
    producer.send("airline", flight_data)
    print(f"Produced: {flight_data}")
    time.sleep(15)
