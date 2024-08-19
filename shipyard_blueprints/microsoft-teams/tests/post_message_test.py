import os
import sys

import pytest
from dotenv import load_dotenv, find_dotenv

from shipyard_microsoft_teams import MicrosoftTeamsClient
from shipyard_microsoft_teams.cli.post_message import main

load_dotenv(find_dotenv())

LONG_MESSAGE = "This is a Long Message: \n"
LONG_MESSAGE += (
    """I know of a song that gets on everybody's nerves, everybody's nerves, everybody's nerves. 
I know of a song that gets on everybody's nerves and this is how it goes."""
    * 10
)

SHORT_MESSAGE = "This is a short message."
SIMPLE_TEST_SCENARIOS = [
    (LONG_MESSAGE, "Long Message: test_happy_path"),
    (SHORT_MESSAGE, "Short Message: test_happy_path"),
    (LONG_MESSAGE, ""),
    (SHORT_MESSAGE, ""),
]


def format_cli_args(
    message_content, message_title="", webhook_url="", team_id="", channel_id=""
):
    return [
        "post_message.py",
        "--message-content",
        message_content,
        "--message-title",
        message_title,
        "--webhook-url",
        webhook_url,
        "--team-id",
        team_id,
        "--channel-id",
        channel_id,
    ]


class TestWebhook:
    @pytest.mark.parametrize("message_content, message_title", SIMPLE_TEST_SCENARIOS)
    def test_happy_path(self, monkeypatch, message_content, message_title):
        monkeypatch.delenv("OAUTH_ACCESS_TOKEN", raising=False)

        webhook_url = os.getenv("MICROSOFT_TEAMS_WEBHOOK_URL")
        if not webhook_url:
            pytest.skip("No webhook URL provided")
        with monkeypatch.context() as patcher:
            patcher.setattr(
                sys,
                "argv",
                format_cli_args(
                    webhook_url=webhook_url,
                    message_content=f"Webhook Test: {message_content}",
                    message_title=message_title,
                ),
            )
            try:
                main()
            except SystemExit as e:
                assert e.code == 0, f"Expected 0 but got {e.code}"

    #
    def test_webhook_url_missing(self, monkeypatch):
        monkeypatch.delenv("OAUTH_ACCESS_TOKEN", raising=False)

        with monkeypatch.context() as patcher:
            patcher.setattr(
                sys,
                "argv",
                format_cli_args(
                    message_content=SHORT_MESSAGE, message_title="Test Title"
                ),
            )

            try:
                main()
            except SystemExit as e:
                assert (
                    e.code == MicrosoftTeamsClient.EXIT_CODE_INVALID_CREDENTIALS
                ), f"Expected {MicrosoftTeamsClient.EXIT_CODE_INVALID_CREDENTIALS} but got {e.code}"


class TestAccessToken:
    @pytest.mark.parametrize("message_content, message_title", SIMPLE_TEST_SCENARIOS)
    def test_happy_path(self, monkeypatch, message_content, message_title):
        with monkeypatch.context() as patcher:
            patcher.setattr(
                sys,
                "argv",
                format_cli_args(
                    message_content=f"Access token test: {message_content}",
                    message_title=message_title,
                    team_id=os.getenv("MICROSOFT_TEAMS_TEAM_ID"),
                    channel_id=os.getenv("MICROSOFT_TEAMS_CHANNEL_ID"),
                    webhook_url="",
                ),
            )

            try:
                main()
            except SystemExit as e:
                assert e.code == 0, f"Expected 0 but got {e.code}"
