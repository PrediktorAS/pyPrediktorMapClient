import asyncio
import aiohttp
import requests
from pydantic import AnyUrl, ValidationError
from typing import Literal, Dict, Any

class Config:
        arbitrary_types_allowed = True

class ClientPool:
    def __init__(self, num_clients: int, rest_url: str, headers: Dict[str, str]):
        self.clients = asyncio.Queue()
        for _ in range(num_clients):
            self.clients.put_nowait(aiohttp.ClientSession(base_url=rest_url, headers=headers))
        self.num_clients = num_clients

    async def get_client(self):
        client = await self.clients.get()
        return client

    async def release_client(self, client):
        await self.clients.put(client)

    async def close_all(self):
        while not self.clients.empty():
            client = await self.clients.get()
            await client.close()

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
    """Function to perform the request to the ModelIndex server

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
        result = request_method.get(combined_url, timeout=request_timeout, params=params, headers=headers)

    if method == "POST":
        result = request_method.post(
            combined_url, data=data, headers=headers, timeout=request_timeout, params=params
        )
    
    if method not in ["GET", "POST"]:
        raise ValidationError("Unsupported method")
    
    result.raise_for_status()

    if 'application/json' in result.headers.get('Content-Type', ''):
        return result.json()

    else:
        return {"error": "Non-JSON response", "content": result.text}
    
async def request_from_api_async(
    client_pool: ClientPool,
    method: str,
    endpoint: str,
    data: str = None,
    params: Dict[str, Any] = None,
    extended_timeout: bool = False,
) -> Dict[str, Any]:
    timeout = aiohttp.ClientTimeout(total=300 if extended_timeout else 30)
    client = await client_pool.get_client()
    
    try:
        if method == "GET":
            async with client.get(endpoint, params=params, timeout=timeout) as response:
                response.raise_for_status()
                if 'application/json' in response.headers.get('Content-Type', ''):
                    return await response.json()
                else:
                    return {"error": "Non-JSON response", "content": await response.text()}
        elif method == "POST":
            async with client.post(endpoint, data=data, params=params, timeout=timeout) as response:
                response.raise_for_status()
                if 'application/json' in response.headers.get('Content-Type', ''):
                    return await response.json()
                else:
                    return {"error": "Non-JSON response", "content": await response.text()}
        else:
            raise ValidationError("Unsupported method")
    finally:
        await client_pool.release_client(client)
