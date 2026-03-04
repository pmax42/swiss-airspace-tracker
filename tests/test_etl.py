import os
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from etl_script import get_proxies, fetch_flight_data

def test_get_proxies_no_env():
    with patch.dict(os.environ, {}, clear=True):
        assert get_proxies() is None

def test_get_proxies_with_env():
    env_vars = {
        "PROXY_HOST": "127.0.0.1",
        "PROXY_PORT": "8080",
        "PROXY_USER": "testuser",
        "PROXY_PASS": "testpass"
    }
    with patch.dict(os.environ, env_vars, clear=True):
        proxies = get_proxies()
        expected_url = "http://testuser:testpass@127.0.0.1:8080"
        assert proxies == {"http": expected_url, "https": expected_url}

@patch("etl_script.requests.post")
@patch("etl_script.requests.get")
def test_fetch_flight_data_success(mock_get, mock_post):
    mock_post_response = MagicMock()
    mock_post_response.status_code = 200
    mock_post_response.json.return_value = {"access_token": "fake_token"}
    mock_post.return_value = mock_post_response

    mock_get_response = MagicMock()
    mock_get_response.status_code = 200
    
    fake_state = [
        "4b1815", "SWR123  ", "Switzerland", 1620000000, 
        1620000000, 8.54, 47.45, 10000.0, 
        False, 250.0, 90.0, 0.0, 
        None, 10200.0, "1234", False, 0
    ]
    mock_get_response.json.return_value = {"states": [fake_state]}
    mock_get.return_value = mock_get_response

    env_vars = {
        "OPENSKY_CLIENT_ID": "fake_client", 
        "OPENSKY_CLIENT_SECRET": "fake_secret"
    }
    
    with patch.dict(os.environ, env_vars):
        df = fetch_flight_data()

    assert not df.empty
    assert isinstance(df, pd.DataFrame)
    assert df.iloc[0]["callsign"] == "SWR123"
    assert df.iloc[0]["origin_country"] == "Switzerland"
    assert "ingestion_time" in df.columns

@patch("etl_script.requests.post")
def test_fetch_flight_data_auth_failure(mock_post):
    mock_post_response = MagicMock()
    mock_post_response.status_code = 401
    mock_post_response.text = "Unauthorized"
    mock_post.return_value = mock_post_response

    env_vars = {
        "OPENSKY_CLIENT_ID": "fake_client", 
        "OPENSKY_CLIENT_SECRET": "fake_secret"
    }

    with patch.dict(os.environ, env_vars):
        df = fetch_flight_data()
        
    assert df.empty