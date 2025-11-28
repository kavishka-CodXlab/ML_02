import argparse, json, datetime, signal, sys, time
from kafka import KafkaConsumer, KafkaProducer
import joblib
import pandas as pd
from prometheus_client import Counter, start_http_server

STOP = False
def sigterm_handler(signal, frame):
    global STOP
    STOP = True
signal.signal(signal.SIGINT, sigterm_handler)
signal.signal(signal.SIGTERM, sigterm_handler)

events_consumed = Counter('events_consumed', 'Number of consumed events')
predictions_made = Counter('predictions_made', 'Number of predictions made')
deadletters = Counter('deadletters', 'Number of deadletters produced')

def load_model(path):
    model = joblib.load(path)
    return model

def to_dataframe(record):
    # record is dict of raw string values -> create single-row DataFrame
    return pd.DataFrame([record])

def process_record(msg_val, model):
    data = json.loads(msg_val)
    # keep customerID for output
    cid = data.get('customerID')
    df = to_dataframe(data)
    # model expects columns matching training features; pipeline includes preprocessing
    proba = model.predict_proba(df)[0][1]
    pred = 'Yes' if proba >= 0.5 else 'No'
    out = {
        "customerID": cid,
        "churn_probability": float(round(proba,4)),
        "prediction": pred,
        "event_ts": data.get('event_ts'),
        "processed_ts": datetime.datetime.utcnow().isoformat()+'Z'
    }
    return out

def streaming_consume(broker, in_topic, out_topic, dead_topic, model_path, metrics_port):
    if metrics_port:
        start_http_server(metrics_port)
    consumer = KafkaConsumer(in_topic, bootstrap_servers=[broker], auto_offset_reset='earliest', group_id='telco-consumer')
    producer = KafkaProducer(bootstrap_servers=[broker])
    model = load_model(model_path)
    while not STOP:
        for m in consumer:
            events_consumed.inc()
            try:
                out = process_record(m.value.decode(), model)
                producer.send(out_topic, key=out['customerID'].encode(), value=json.dumps(out).encode())
                predictions_made.inc()
                print(f"Prediction: {out['prediction']} for {out['customerID']}")
            except Exception as e:
                # produce deadletter
                payload = {"error": str(e), "raw": m.value.decode()}
                producer.send(dead_topic, key=m.key, value=json.dumps(payload).encode())
                deadletters.inc()
            producer.flush()
            if STOP:
                break
        time.sleep(0.1)

def batch_consume(broker, in_topic, out_topic, dead_topic, model_path, window, metrics_port):
    # Poll messages and process in batches of size 'window'
    if metrics_port:
        start_http_server(metrics_port)
    consumer = KafkaConsumer(in_topic, bootstrap_servers=[broker], auto_offset_reset='earliest', group_id='telco-consumer-batch', consumer_timeout_ms=5000)
    producer = KafkaProducer(bootstrap_servers=[broker])
    model = load_model(model_path)
    batch=[]
    for m in consumer:
        batch.append(m)
        if len(batch) >= window:
            for msg in batch:
                events_consumed.inc()
                try:
                    out = process_record(msg.value.decode(), model)
                    producer.send(out_topic, key=out['customerID'].encode(), value=json.dumps(out).encode())
                    predictions_made.inc()
                except Exception as e:
                    payload = {"error": str(e), "raw": msg.value.decode()}
                    producer.send(dead_topic, key=msg.key, value=json.dumps(payload).encode())
                    deadletters.inc()
            producer.flush()
            print("Processed batch of", len(batch))
            batch=[]
    # process remaining
    for msg in batch:
        events_consumed.inc()
        try:
            out = process_record(msg.value.decode(), model)
            producer.send(out_topic, key=out['customerID'].encode(), value=json.dumps(out).encode())
            predictions_made.inc()
        except Exception as e:
            payload = {"error": str(e), "raw": msg.value.decode()}
            producer.send(dead_topic, key=msg.key, value=json.dumps(payload).encode())
            deadletters.inc()
    producer.flush()
    print("Batch consumer finished")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--mode', choices=['streaming','batch'], required=True)
    ap.add_argument('--broker', default='localhost:9092')
    ap.add_argument('--in-topic', default='telco.raw.customers')
    ap.add_argument('--out-topic', default='telco.churn.predictions')
    ap.add_argument('--dead-topic', default='telco.deadletter')
    ap.add_argument('--model', default='model.joblib')
    ap.add_argument('--window', type=int, default=500)
    ap.add_argument('--metrics-port', type=int, default=8000)
    args = ap.parse_args()

    if args.mode == 'streaming':
        streaming_consume(args.broker, args.in_topic, args.out_topic, args.dead_topic, args.model, args.metrics_port)
    else:
        batch_consume(args.broker, args.in_topic, args.out_topic, args.dead_topic, args.model, args.window, args.metrics_port)

if __name__=='__main__':
    main()
