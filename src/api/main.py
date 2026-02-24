from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
import pandas as pd
from src.api.model import predict_churn

app = FastAPI(title="GlobalRetail 360 Churn API")

class CustomerFeatures(BaseModel):
    segment: str
    region: str
    total_orders: float
    total_sales: float
    avg_discount: float
    total_returns: float

@app.post("/predict")
def predict(customers: List[CustomerFeatures]):
    # Convert list of Pydantic objects to DataFrame
    df = pd.DataFrame([c.dict() for c in customers])
    # Get predictions
    probabilities = predict_churn(df)
    # Return as list
    return {"predictions": probabilities.tolist()}