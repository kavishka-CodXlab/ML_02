import pandas as pd
from joblib import load
import os

def test_model_pipeline_runs():
    # Ensure we can find the files whether running from root or tests/
    model_path = 'model.joblib' if os.path.exists('model.joblib') else '../model.joblib'
    data_path = 'data/Telco-Customer-Churn.csv' if os.path.exists('data/Telco-Customer-Churn.csv') else '../data/Telco-Customer-Churn.csv'
    
    model = load(model_path)
    # build a tiny row with required columns by loading one from CSV
    df = pd.read_csv(data_path, nrows=1)
    df['TotalCharges'] = pd.to_numeric(df['TotalCharges'], errors='coerce')

    # The model pipeline expects features without customerID and Churn
    preds = model.predict_proba(df.drop(columns=['customerID','Churn']))
    assert preds.shape[0] == 1
