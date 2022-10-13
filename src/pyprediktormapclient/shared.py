import requests
from pydantic import HttpUrl, validate_arguments
from typing import Literal


@validate_arguments
def request_from_api(
    rest_url: HttpUrl,
    method: Literal["GET", "POST"],
    endpoint: str,
    data: str = None,
    headers: dict = None,
    extended_timeout: bool = False,
) -> str:
    """Function to perform the request to the ModelIndex server

    Args:
        rest_url (str): The URL with trailing shash
        method (str): "GET" or "POST"
        endpoint (str): The last part of the url (without the leading slash)
        data (str): defaults to None but can contain the data to send to the endpoint
        headers (str): default to None but can contain the headers og the request
    Returns:
        JSON: The result if successfull

    Todo:
        * Add logging
    """
    request_timeout = (3, 300 if extended_timeout else 27)
    if method == "GET":
        result = requests.get(rest_url + endpoint, timeout=request_timeout)

    if method == "POST":
        result = requests.post(
            rest_url + endpoint, data=data, headers=headers, timeout=request_timeout
        )

    result.raise_for_status()
    return result.json()
