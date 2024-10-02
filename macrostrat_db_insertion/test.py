# Test the insertion of data into the macrostrat database

import json

import pytest
from fastapi.testclient import TestClient

from macrostrat_db_insertion.server import app


@pytest.fixture
def api_client() -> TestClient:
    with TestClient(app) as api_client:
        yield api_client


class TestAPI:

    def test_insert(self, api_client: TestClient):
        data = json.loads(open("example_request.json", "r").read())

        response = api_client.post(
            "/record_run",
            json=data
        )

        assert response.status_code == 200
