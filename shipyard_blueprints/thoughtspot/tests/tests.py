import os
from shipyard_thoughtspot import ThoughtSpotClient


def test_connection():
    password = os.environ.get("THOUGHTSPOT_PASSWORD")
    token = os.getenv("THOUGHTSPOT_TOKEN")
    client = ThoughtSpotClient(token)
    response = client.connect("bpuser@shipyardapp.com", password)
    return response.json()


def test_export_answer():
    token = os.getenv("THOUGHTSPOT_TOKEN")
    client = ThoughtSpotClient(token)
    response = client.get_answer_data(
        metadata_identifier="b95b0bce-b036-4a0e-9af0-ba00621f9696", file_format="csv"
    )
    return response


def test_export_answer_report():
    token = os.getenv("THOUGHTSPOT_TOKEN")
    client = ThoughtSpotClient(token)
    response = client.get_answer_report(
        metadata_identifier="b95b0bce-b036-4a0e-9af0-ba00621f9696", file_format="csv"
    )
    return response


def test_export_live_report(format="png"):
    token = os.getenv("THOUGHTSPOT_TOKEN")
    client = ThoughtSpotClient(token)
    response = client.get_live_report(
        metadata_identifier="ac732138-d3e9-4dab-9ffb-74b85cbca7b9",
        file_format=format,
        file_name="live_report"
    )
    return response

def test_export_live_report_pdf(format = 'pdf'):
    token = os.getenv("THOUGHTSPOT_TOKEN")
    client = ThoughtSpotClient(token)
    response = client.get_live_report(
        metadata_identifier="ac732138-d3e9-4dab-9ffb-74b85cbca7b9",
        file_format=format,
        file_name="live_report"
    )
    return response


def test_metadata_export():
    token = os.getenv("THOUGHTSPOT_TOKEN")
    client = ThoughtSpotClient(token)
    response = client.get_metadata(["b95b0bce-b036-4a0e-9af0-ba00621f9696"])


def test_search_data():
    token = os.getenv("THOUGHTSPOT_TOKEN")
    client = ThoughtSpotClient(token)
    resp = client.search_data(
        table_identifier="70ae71aa-df03-427d-88ad-22834d0cc741",
        query="[Home Goals] by [Home Team]",
        file_format="csv",
    )
    return resp


if __name__ == "__main__":
    conn = test_connection()
    resp_answer = test_export_answer_report()
    resp_live = test_export_live_report()
    resp_meta = test_metadata_export()
    resp_search = test_search_data()
