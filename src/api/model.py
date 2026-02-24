import joblib
import os
import pandas as pd

MODEL_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../src/models/logistic_churn_pipeline.pkl")
)

pipeline = joblib.load(MODEL_PATH)

def predict_churn(input_data):

    input_df = pd.DataFrame(input_data)

    prob = pipeline.predict_proba(input_df)[:, 1]
    return prob.tolist()