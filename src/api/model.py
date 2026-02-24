import joblib
import os

MODEL_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src/models/logistic_churn_pipeline.pkl"))

pipeline = joblib.load(MODEL_PATH)

def predict_churn(input_df):
    """
    input_df: pandas DataFrame with columns:
        segment, region, total_orders, total_sales, avg_discount, total_returns
    returns: probability of churn for each row
    """
    prob = pipeline.predict_proba(input_df)[:, 1]
    return prob