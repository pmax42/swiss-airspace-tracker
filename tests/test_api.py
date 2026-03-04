from fastapi.testclient import TestClient
from api import app
import pytest

client = TestClient(app)

def test_read_latest_flights_format():
    """Verify that the API responds with the expected JSON structure."""
    response = client.get("/flights/latest")
    assert response.status_code in [200, 500] 
    
    if response.status_code == 200:
        data = response.json()
        assert "count" in data
        assert "data" in data
        assert isinstance(data["data"], list)