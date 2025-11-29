# Telco Customer Churn Kafka Pipeline

This project implements a real-time data pipeline for predicting customer churn using Kafka, Python, and Scikit-Learn.

## 📂 Deliverables Checklist

- [x] **Producer Script**: `scripts/producer.py` (Batch & Streaming modes)
- [x] **Consumer Script**: `scripts/consumer.py` (Predictions & Metrics)
- [x] **Training Script**: `scripts/train_model.py`
- [x] **Trained Model**: `model.joblib`
- [x] **Docker Compose**: `docker-compose.yml` (Kafka, Zookeeper, Kafdrop)
- [x] **Airflow DAGs**:
    - `dags/telco_batch_pipeline.py`
    - `dags/telco_streaming_consumer.py`
- [x] **Tests**:
    - `tests/test_transform.py` (Unit)
    - `tests/test_integration.py` (Integration)

## 🚀 Setup & Installation

1.  **Start Kafka Stack:**
    ```bash
    docker-compose up -d
    ```
    - Kafka: `localhost:9094` (External), `kafka:29092` (Internal)
    - Kafdrop UI: [http://localhost:9000](http://localhost:9000)

2.  **Install Dependencies:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

## 🏃‍♂️ How to Run

### 1. Train the Model
```bash
python scripts/train_model.py --csv data/Telco-Customer-Churn.csv --out model.joblib
```

### 2. Streaming Mode (Real-time)
**Terminal 1: Start Consumer**
```bash
python scripts/consumer.py --mode streaming --model model.joblib --metrics-port 8000 --broker localhost:9094
```

**Terminal 2: Start Producer**
```bash
python scripts/producer.py --mode streaming --csv data/Telco-Customer-Churn.csv --events-per-sec 2 --broker localhost:9094
```

### 3. Batch Mode
```bash
python scripts/producer.py --mode batch --csv data/Telco-Customer-Churn.csv --batch-size 500 --broker localhost:9094
python scripts/consumer.py --mode batch --window 1000 --model model.joblib --broker localhost:9094
```

## 📊 Monitoring & Verification

- **Summary Script:**
    ```bash
    python scripts/generate_summary.py --broker localhost:9094
    ```
- **Health Check:**
    ```bash
    python scripts/kafka_health_check.py --broker localhost:9094
    ```
- **Prometheus Metrics:** [http://localhost:8000/metrics](http://localhost:8000/metrics)
- **Kafdrop (Topics):** [http://localhost:9000](http://localhost:9000)

##  Testing

Run unit and integration tests:
```bash
pytest tests/test_transform.py tests/test_integration.py
```

## 📝 Sample Output

**Prediction Message (`telco.churn.predictions`):**
```json
{
  "customerID": "7590-VHVEG",
  "churn_probability": 0.8231,
  "prediction": "Yes",
  "event_ts": "2025-10-03T04:00:00Z",
  "processed_ts": "2025-10-03T04:00:01Z"
}
```
