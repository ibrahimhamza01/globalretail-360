from fastapi import FastAPI
from typing import List
from src.api.schemas import CustomerInput, PredictionResponse
from src.api.model import predict_churn

app = FastAPI(title="GlobalRetail 360 Churn API")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/predict", response_model=PredictionResponse)
def predict(customers: List[CustomerInput]):
    input_data = [c.model_dump() for c in customers]
    predictions = predict_churn(input_data)
    return {"predictions": predictions}