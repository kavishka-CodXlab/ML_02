from kafka import KafkaProducer, KafkaConsumer
import json, time
import pytest

def test_produce_consume_roundtrip():
    # Using localhost:9094 as configured in docker-compose
    producer = KafkaProducer(bootstrap_servers=['localhost:9094'])
    consumer = KafkaConsumer('telco.churn.predictions', bootstrap_servers=['localhost:9094'], auto_offset_reset='earliest', consumer_timeout_ms=5000)
    
    # send a sample message directly to raw topic
    # Use a unique ID to ensure we find THIS specific message
    sample = {"customerID":"test-integration-2","gender":"Female","SeniorCitizen":0,"Partner":"No","Dependents":"No","tenure":1,"PhoneService":"No","MultipleLines":"No phone service","InternetService":"DSL","OnlineSecurity":"No","OnlineBackup":"Yes","DeviceProtection":"No","TechSupport":"No","StreamingTV":"No","StreamingMovies":"No","Contract":"Month-to-month","PaperlessBilling":"Yes","PaymentMethod":"Electronic check","MonthlyCharges":29.85,"TotalCharges":29.85,"Churn":"No","event_ts":"2025-10-03T04:00:00Z"}
    
    producer.send('telco.raw.customers', key=b'test-integration-2', value=json.dumps(sample).encode())
    producer.flush()
    
    # Now wait a bit for consumer service to process (you must run consumer in background)
    time.sleep(5)
    
    # check predictions
    found=False
    for m in consumer:
        v=json.loads(m.value.decode())
        if v.get('customerID')=='test-integration-2':
            found=True
            break
    assert found
