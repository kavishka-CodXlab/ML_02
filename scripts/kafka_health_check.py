from kafka import KafkaAdminClient
import argparse
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--broker', default='localhost:9092')
    args = parser.parse_args()
    try:
        admin = KafkaAdminClient(bootstrap_servers=[args.broker], request_timeout_ms=2000)
        topics = admin.list_topics()
        print("OK - topics:", topics)
    except Exception as e:
        print("ERR", e)
        raise

if __name__ == "__main__":
    main()
