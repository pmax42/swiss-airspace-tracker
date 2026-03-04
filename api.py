import os
import logging
from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")

DB_CONNECTION_STR = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}"

app = FastAPI(
    title="Swiss Air Traffic API",
    description="Read-only API for real-time flight data",
    version="1.0.0"
)

engine = create_engine(DB_CONNECTION_STR)

@app.get("/flights/latest")
def get_latest_flights() -> dict:
    """Retrieves the most recent batch of flight data from the database."""
    try:
        with engine.connect() as conn:
            query_time = text("SELECT ingestion_time FROM flights ORDER BY ingestion_time DESC LIMIT 1")
            latest_time = conn.execute(query_time).scalar()
            
            if not latest_time:
                return {"count": 0, "data": []}

            query_flights = text("SELECT * FROM flights WHERE ingestion_time = :time")
            result_proxy = conn.execute(query_flights, {"time": latest_time}).mappings().all()
            
            flights_data = [dict(row) for row in result_proxy]
            
        return {
            "latest_ingestion": str(latest_time),
            "count": len(flights_data),
            "data": flights_data
        }

    except Exception as e:
        logging.error(f"Error fetching latest flights: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")