import os
import json
import random
import time
from datetime import datetime, timezone
from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv

load_dotenv()

CONNECTION_STRING = os.getenv("AZURE_EVENTHUB_CONNECTION_STRING")
EVENTHUB_NAME = os.getenv("AZURE_EVENTHUB_NAME")

# Services possibles
SERVICES = ["DSL", "Fiber optic", "No"]
CONTRACTS = ["Month-to-month", "One year", "Two year"]
PAYMENT_METHODS = ["Electronic check", "Mailed check", "Bank transfer", "Credit card"]

def generate_event(customer_id: int) -> dict:
    return {
        "customer_id": f"CUST-{customer_id:05d}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "internet_service": random.choice(SERVICES),
        "contract": random.choice(CONTRACTS),
        "payment_method": random.choice(PAYMENT_METHODS),
        "monthly_charges": round(random.uniform(20, 120), 2),
        "tenure_months": random.randint(1, 72),
        "support_calls": random.randint(0, 10),
        "late_payments": random.randint(0, 5),
    }

def produce_events(n_events: int = 100, delay: float = 0.05):
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        eventhub_name=EVENTHUB_NAME
    )
    print(f"Envoi de {n_events} événements vers Event Hubs...")
    with producer:
        batch = producer.create_batch()
        for i in range(n_events):
            event = generate_event(i)
            try:
                batch.add(EventData(json.dumps(event)))
            except ValueError:
                # Batch plein — envoie et recrée
                producer.send_batch(batch)
                batch = producer.create_batch()
                batch.add(EventData(json.dumps(event)))
            time.sleep(delay)
        producer.send_batch(batch)
    print(f"✓ {n_events} événements envoyés.")

if __name__ == "__main__":
    produce_events(n_events=100)