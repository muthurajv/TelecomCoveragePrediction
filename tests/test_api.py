"""API unit tests using FastAPI TestClient (no live BQ connection)."""

from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from src.api.main import app

client = TestClient(app)


def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_get_score_not_found():
    with patch("src.api.main.bq") as mock_bq:
        mock_bq.return_value.query.return_value.result.return_value = iter([])
        response = client.get("/score/8928308280fffff")
    assert response.status_code == 404


def test_get_ranked_list_not_found():
    with patch("src.api.main.bq") as mock_bq:
        mock_bq.return_value.query.return_value.result.return_value = iter([])
        response = client.get("/ranked-list?market_id=nyc&top_n=10")
    assert response.status_code == 404


def test_openapi_schema_has_all_endpoints():
    response = client.get("/openapi.json")
    assert response.status_code == 200
    paths = response.json()["paths"]
    assert "/health" in paths
    assert "/score/{h3_cell_id}" in paths
    assert "/ranked-list" in paths
    assert "/scenario" in paths
