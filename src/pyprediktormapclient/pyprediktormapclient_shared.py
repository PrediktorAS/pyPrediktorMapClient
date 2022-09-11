import requests

def request_from_api(rest_url: str, method: str, endpoint: str, data=None, headers=None):
    """Function to perform the request to the ModelIndex server

    Args:
        method (str): "GET" or "POST"
        endpoint (str): The last part of the url (without the leading "/") 
        data (str): defaults to None but can contain the data to send to the endpoint
        headers (str): default to None but can contain the headers og the request
    Returns:
        JSON: The result if successfull
    """
    timeout = (3, 27)
    if method == 'GET':
        result = requests.get(rest_url + endpoint, timeout=timeout)
    elif method == 'POST':
        result = requests.post(rest_url + endpoint, data=data, headers=headers, timeout=timeout)
    else:
        raise Exception('Method not supported')
    result.raise_for_status()
    return result.json()