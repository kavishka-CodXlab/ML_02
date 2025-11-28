import argparse, csv, json, random, time, datetime, signal, sys
from kafka import KafkaProducer
from pathlib import Path

STOP = False
def sigterm_handler(signal, frame):
    global STOP
    STOP = True
signal.signal(signal.SIGINT, sigterm_handler)
signal.signal(signal.SIGTERM, sigterm_handler)

def load_rows(csv_path):
    rows=[]
    with open(csv_path, newline='') as f:
        reader=csv.DictReader(f)
        for r in reader:
            # convert numeric fields
            if r.get('MonthlyCharges'): r['MonthlyCharges']=float(r['MonthlyCharges'])
            if r.get('TotalCharges'):
                try:
                    r['TotalCharges']=float(r['TotalCharges'])
                except:
                    r['TotalCharges']=0.0
            rows.append(r)
    return rows

def streaming_mode(producer, topic, rows, eps):
    interval = 1.0 / max(eps, 0.0001)
    while not STOP:
        r = random.choice(rows).copy()
        r['event_ts'] = datetime.datetime.utcnow().isoformat()+'Z'
        key = str(r['customerID']).encode()
        producer.send(topic, key=key, value=json.dumps(r).encode())
        producer.flush()
        print(f"Sent: {r['customerID']}")
        time.sleep(interval)

def batch_mode(producer, topic, csv_path, batch_size, checkpoint_file):
    rows = load_rows(csv_path)
    cp = Path(checkpoint_file)
    last_idx = int(cp.read_text()) if cp.exists() else 0
    for i in range(last_idx, len(rows), batch_size):
        if STOP:
            break
        batch = rows[i:i+batch_size]
        for r in batch:
            r['event_ts'] = datetime.datetime.utcnow().isoformat()+'Z'
            producer.send(topic, key=str(r['customerID']).encode(), value=json.dumps(r).encode())
        producer.flush()
        cp.write_text(str(i+batch_size))
        print(f"Sent up to {i+batch_size}")
    print("Batch done")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--mode', choices=['streaming','batch'], required=True)
    ap.add_argument('--csv', default='../data/Telco-Customer-Churn.csv')
    ap.add_argument('--broker', default='localhost:9092')
    ap.add_argument('--topic', default='telco.raw.customers')
    ap.add_argument('--events-per-sec', type=float, default=1.0)
    ap.add_argument('--batch-size', type=int, default=200)
    ap.add_argument('--checkpoint', default='.producer.ckpt')
    args = ap.parse_args()

    producer = KafkaProducer(bootstrap_servers=[args.broker])
    if args.mode=='streaming':
        rows = load_rows(args.csv)
        streaming_mode(producer, args.topic, rows, args.events_per_sec)
    else:
        batch_mode(producer, args.topic, args.csv, args.batch_size, args.checkpoint)

if __name__=='__main__':
    main()
