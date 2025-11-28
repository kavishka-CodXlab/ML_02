from kafka import KafkaConsumer
import json
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--broker', default='localhost:9092')
    parser.add_argument('--topic', default='telco.churn.predictions')
    args = parser.parse_args()

    consumer = KafkaConsumer(args.topic, bootstrap_servers=[args.broker], auto_offset_reset='earliest', consumer_timeout_ms=1000)
    cnt=0
    yes=0
    for m in consumer:
        cnt+=1
        v=json.loads(m.value.decode())
        if v.get('prediction')=='Yes':
            yes+=1
    print("Total predictions:", cnt, "Yes:", yes, "No:", cnt-yes)

if __name__ == "__main__":
    main()
