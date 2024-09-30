from typing import Literal

import requests
from pydantic import AnyUrl, ValidationError


class Config:
    arbitrary_types_allowed = True


def request_from_api(
    rest_url: AnyUrl,
    method: Literal["GET", "POST"],
    endpoint: str,
    data: str = None,
    params: dict = None,
    headers: dict = None,
    extended_timeout: bool = False,
    session: requests.Session = None,
) -> str:
    """Function to perform the request to the ModelIndex server.

    Args:
        rest_url (str): The URL with trailing shash
        method (str): "GET" or "POST"
        endpoint (str): The last part of the url (without the leading slash)
        data (str): defaults to None but can contain the data to send to the endpoint
        headers (str): default to None but can contain the headers og the request
    Returns:
        JSON: The result if successfull
    """
    request_timeout = (3, 300 if extended_timeout else 27)
    combined_url = f"{rest_url}{endpoint}"

    # Use session if provided, else use requests
    request_method = session if session else requests

    if method == "GET":
        result = request_method.get(
            combined_url,
            timeout=request_timeout,
            params=params,
            headers=headers,
        )

    if method == "POST":
        result = request_method.post(
            combined_url,
            data=data,
            headers=headers,
            timeout=request_timeout,
            params=params,
        )

    if method not in ["GET", "POST"]:
        raise ValidationError("Unsupported method")

    result.raise_for_status()

    if "application/json" in result.headers.get("Content-Type", ""):
        return result.json()

    else:
        return {"error": "Non-JSON response", "content": result.text}
