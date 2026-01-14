import os
import pytest
from unittest import mock
from airflow.models import Variable, Connection, DagBag

@pytest.fixture
def api_key():
    with mock.patch.dict("os.environ", {"AIRFLOW_VAR_API_KEY": "MOCK_KEY123"}):
        yield Variable.get("API_KEY")
    
@pytest.fixture
def dagbag():
    yield DagBag()

@pytest.fixture()
def airflow_variable():
    def get_airflow_variable(variable_name):
        env_var = f"AIRFLOW_VAR_{variable_name.upper()}"
        return os.getenv(env_var)
    
    return get_airflow_variable
