from pydantic import BaseModel, Field, field_validator
from typing import ClassVar, List

class CustomerInput(BaseModel):
    segment: str = Field(..., example="Consumer")
    region: str = Field(..., example="Central Us")
    total_orders: float
    total_sales: float
    avg_discount: float
    total_returns: float

    # ---- Allowed Values ----
    VALID_SEGMENTS: ClassVar[set[str]] = [
        "Consumer",
        "Corporate",
        "Home Office"
    ]

    VALID_REGIONS: ClassVar[set[str]] = [
        "Caribbean",
        "Eastern Africa",
        "Southern Europe",
        "Eastern Asia",
        "South America",
        "Oceania",
        "Southern Us",
        "North Africa",
        "Western Africa",
        "Southern Asia",
        "Eastern Europe",
        "Central Africa",
        "Eastern Us",
        "Southeastern Asia",
        "Central America",
        "Central Asia",
        "Canada",
        "Western Asia",
        "Western Europe",
        "Northern Europe",
        "Western Us",
        "Central Us",
        "Southern Africa",
    ]

    @field_validator("region")
    @classmethod
    def validate_region(cls, v: str) -> str:
        if not isinstance(v, str):
            raise ValueError("Region must be a string")

        normalized = v.strip().title()

        if normalized not in cls.VALID_REGIONS:
            raise ValueError(f"Invalid region: {v}")

        return normalized

    @field_validator("segment")
    @classmethod
    def validate_segment(cls, v: str) -> str:
        if not isinstance(v, str):
            raise ValueError("Segment must be a string")

        normalized = v.strip().title()

        if normalized not in cls.VALID_SEGMENTS:
            raise ValueError(f"Invalid segment: {v}")

        return normalized

    @field_validator("total_orders", "total_sales", "avg_discount", "total_returns")
    @classmethod
    def non_negative_values(cls, v: float) -> float:
        if v < 0:
            raise ValueError("Numeric values must be non-negative")
        return v

    @field_validator("avg_discount")
    @classmethod
    def discount_range(cls, v: float) -> float:
        if not 0 <= v <= 1:
            raise ValueError("avg_discount must be between 0 and 1")
        return v
    
class PredictionResponse(BaseModel):
    predictions: List[float]