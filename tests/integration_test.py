import requests
import pytest

def test_youtube_api_response(airflow_variable):
    api_key = airflow_variable("api_key")
    channel_handle = airflow_variable("channel_handle")

    url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_handle}&key={api_key}"

    try: 
        response = requests.get(url)
        assert response.status_code == 200
    
    except requests.RequestException as e:
        pytest.fail(f"Request to Youtube API failed: {e}")