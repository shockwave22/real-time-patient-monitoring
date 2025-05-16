from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

PATIENT_IDS = [f"P{i:03}" for i in range(1, 11)]

def generate_vitals():
    return {
        "patient_id": random.choice(PATIENT_IDS),
        "heart_rate": random.randint(60, 150),
        "bp_systolic": random.randint(90, 180),
        "bp_diastolic": random.randint(60, 120),
        "timestamp": time.time()
    }

while True:
    vitals = generate_vitals()
    producer.send("vitals", value=vitals)
    print("Sent:", vitals)
    time.sleep(1)
