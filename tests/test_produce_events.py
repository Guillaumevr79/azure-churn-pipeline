import json
from datetime import datetime
import sys
import os

# Ajoute le dossier scripts au path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingestion", "scripts"))

from importlib import import_module

# Import du module (nom avec tiret → import dynamique)
produce = import_module("produce-events")
generate_event = produce.generate_event
SERVICES = produce.SERVICES
CONTRACTS = produce.CONTRACTS
PAYMENT_METHODS = produce.PAYMENT_METHODS


def test_event_has_all_keys():
    event = generate_event(1)
    expected_keys = {
        "customer_id", "timestamp", "internet_service", "contract",
        "payment_method", "monthly_charges", "tenure_months",
        "support_calls", "late_payments"
    }
    assert set(event.keys()) == expected_keys


def test_customer_id_format():
    event = generate_event(42)
    assert event["customer_id"] == "CUST-00042"


def test_customer_id_padding():
    assert generate_event(0)["customer_id"] == "CUST-00000"
    assert generate_event(99999)["customer_id"] == "CUST-99999"


def test_categorical_values_valid():
    for _ in range(100):
        event = generate_event(1)
        assert event["internet_service"] in SERVICES
        assert event["contract"] in CONTRACTS
        assert event["payment_method"] in PAYMENT_METHODS


def test_numeric_ranges():
    for _ in range(100):
        event = generate_event(1)
        assert 20 <= event["monthly_charges"] <= 120
        assert 1 <= event["tenure_months"] <= 72
        assert 0 <= event["support_calls"] <= 10
        assert 0 <= event["late_payments"] <= 5


def test_timestamp_is_iso():
    event = generate_event(1)
    # Ne lève pas d'exception si format ISO valide
    datetime.fromisoformat(event["timestamp"])


def test_event_is_json_serializable():
    event = generate_event(1)
    # Doit pouvoir être sérialisé sans erreur (c'est ce que fait le producer)
    json.dumps(event)


def test_monthly_charges_rounded():
    for _ in range(50):
        event = generate_event(1)
        # Vérifie max 2 décimales
        assert round(event["monthly_charges"], 2) == event["monthly_charges"]