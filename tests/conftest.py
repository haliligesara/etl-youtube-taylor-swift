import os
import pytest
from unittest import mock
from airflow.models import Variable, Connection, DagBag

@pytest.fixture
def api_key():
    with mock.patch.dict("os.environ",AIRFLOW_VAR_API_KEY="MOCK_KEY123"):
        yield Variable.get("API_KEY")


@pytest.fixture
def mock_postgres_conn_vars():
    conn = Connection(
        login="mock_username",
        password="mock_password",
        host="mock_host",
        port="1234",
        schema="mock_db_name",
    )
    conn_uri = conn.get_uri()

    with mock.patch.dict("os.environ", AIRFLOW_CONN_POSTGRES_DB_YT_ELT=conn_uri):
        yield Connection.get_connection_from_secrets(conn_id="POSTGRES_DB_YT_ELT")
    
@pytest.fixture
def dagbag():
    yield DagBag()

@pytest.fixture()
def airflow_variable():
    def get_airflow_variable(variable_name):
        env_var = f"AIRFLOW_VAR_{variable_name.upper}"
        return os.getenv(env_var)
    
    return get_airflow_variable