import os
import re
from urllib import parse
from shipyard_templates import ShipyardLogger

logger = ShipyardLogger.get_logger()


def convert_to_boolean(string: str) -> bool:
    """
    Convert a string to a boolean.

    Args:
    string (str): A string to convert to a boolean.

    Returns:
    bool: True if the string is "TRUE" (case-insensitive) else False.
    """
    logger.debug(f"Converting {string} to boolean...")

    bool_string = string.upper().strip()
    if bool_string == "TRUE":
        result = True
    elif bool_string == "FALSE":
        result = False
    else:
        raise ValueError(f"Invalid value for boolean: {string}")
    logger.debug(f"Converted {string} to {result} boolean.")
    return result


def create_shipyard_link() -> str:
    """
    Create a link back to the Shipyard log page for the currently running voyage.

    This function generates a URL that links to the Shipyard log page. It uses
    environment variables to construct this link. If all required environment variables
    are not set, a generic Shipyard URL is returned.

    Returns:
    str: A URL string that links to the Shipyard log page for the current voyage,
         or a generic Shipyard URL if the necessary details are not available.
    """
    logger.debug("Creating Shipyard link...")

    base_url = "https://app.shipyardapp.com/"
    org_name = os.getenv("SHIPYARD_ORG_NAME")
    project_id = os.getenv("SHIPYARD_PROJECT_ID")
    fleet_id = os.getenv("SHIPYARD_FLEET_ID")
    vessel_id = os.getenv("SHIPYARD_VESSEL_ID")
    fleet_log_id = os.getenv("SHIPYARD_FLEET_LOG_ID")
    vessel_log_id = os.getenv("SHIPYARD_LOG_ID")

    if project_id and fleet_id and fleet_log_id:
        logger.debug("Detecting project_id, fleet_id, and fleet_log_id...")
        dynamic_link_section = f"{org_name}/projects/{project_id}/fleets/{fleet_id}/logs/{fleet_log_id}/{vessel_log_id}"
    elif project_id and vessel_id and vessel_log_id:
        logger.debug("Detecting project_id, vessel_id, and vessel_log_id...")
        dynamic_link_section = (
            f"{org_name}/projects/{project_id}/vessels/{vessel_id}/logs/{vessel_log_id}"
        )
    else:
        logger.debug(
            "No project_id, fleet_id, fleet_log_id, vessel_id, or vessel_log_id detected. Using generic link..."
        )
        return "www.shipyardapp.com"
    shipyard_link = base_url + parse.quote(dynamic_link_section)

    logger.debug(f"Created Shipyard link: {shipyard_link}")

    return shipyard_link
