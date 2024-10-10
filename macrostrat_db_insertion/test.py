# Test the insertion of data into the macrostrat database

import json
import glob
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from macrostrat_db_insertion.server import app
from macrostrat_db_insertion.security import get_groups_from_header_token

TEST_GROUP_TOKEN = "vFWCCodpP8hFF6LxFrpYQTqcJjCGOWyn"
TEST_GROUP_ID = 2


@pytest.fixture
def api_client() -> TestClient:
    with TestClient(app) as api_client:
        yield api_client


class TestAPI:

    def test_insert(self, api_client: TestClient):

        for file in glob.glob("example_requests/**/*.json"):
            data = json.loads(open(file, "r").read())

            response = api_client.post(
                "/record_run",
                json=data
            )

            assert response.status_code == 200


class TestSecurity:

    def test_get_groups_from_header_token(self, api_client: TestClient):

        mock_header = SimpleNamespace(**{
            'credentials': TEST_GROUP_TOKEN
        })

        assert get_groups_from_header_token(mock_header) == TEST_GROUP_ID
