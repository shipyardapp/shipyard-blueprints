import os
import sys

from shipyard_templates import ShipyardLogger

from shipyard_microsoft_teams import MicrosoftTeamsClient

logger = ShipyardLogger.get_logger()


def main():
    logger.setLevel("AUTHTEST")

    sys.exit(
        MicrosoftTeamsClient(webhook_url=os.getenv("MICROSOFT_TEAMS_WEBHOOK_URL")).connect()
    )


if __name__ == "__main__":
    main()
