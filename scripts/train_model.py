import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
import joblib
import argparse

def load_data(path):
    df = pd.read_csv(path)
    # Convert TotalCharges possibly to numeric (some blanks)
    df['TotalCharges'] = pd.to_numeric(df['TotalCharges'], errors='coerce')
    return df

def build_pipeline(cat_cols, num_cols):
    cat_pipe = Pipeline([
        ('imputer', SimpleImputer(strategy='constant', fill_value='missing')),
        ('onehot', OneHotEncoder(handle_unknown='ignore', sparse=False))
    ])
    num_pipe = Pipeline([
        ('imputer', SimpleImputer(strategy='median')),
        ('scaler', StandardScaler())
    ])
    preproc = ColumnTransformer([
        ('cats', cat_pipe, cat_cols),
        ('nums', num_pipe, num_cols)
    ])
    pipe = Pipeline([
        ('preproc', preproc),
        ('clf', RandomForestClassifier(n_estimators=200, random_state=42, n_jobs=-1))
    ])
    return pipe

def main(args):
    df = load_data(args.csv)
    # target
    y = df['Churn'].map({'Yes': 1, 'No': 0})
    # features - choose relevant columns (exclude customerID and Churn)
    drop_cols = ['customerID', 'Churn']
    X = df.drop(columns=drop_cols)
    # identify categorical and numeric
    num_cols = X.select_dtypes(include=['int64','float64']).columns.tolist()
    cat_cols = X.select_dtypes(include=['object']).columns.tolist()
    # build pipeline
    pipe = build_pipeline(cat_cols, num_cols)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    pipe.fit(X_train, y_train)
    # save model
    joblib.dump(pipe, args.out)
    print("Saved model to", args.out)
    # print simple accuracy
    print("Train score:", pipe.score(X_train, y_train))
    print("Test score:", pipe.score(X_test, y_test))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv', default='../data/Telco-Customer-Churn.csv')
    parser.add_argument('--out', default='model.joblib')
    args = parser.parse_args()
    main(args)
