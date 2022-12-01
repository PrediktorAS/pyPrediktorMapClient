# Import the required packeages
import datetime
import http.cookies
import json
from urllib.parse import urlparse
import requests
# Import OPC UA functions
from pyprediktormapclient.opc_ua import OPC_UA, Variables, WriteHistoricalVariables, Value, SubValue
from pyprediktormapclient.ory_client import ORY_CLIENT

def main():
    namespace_list = []
    # Read credentials and server url from secrets file
    f = open ('src/pyprediktormapclient/secrets.cfg', "r")

    # Reading from secret file with credentials
    api_config = json.loads(f.read())
    
    opcua_rest_url = api_config.get('opcua_rest_url')
    opcua_server_url = api_config.get('opcua_server_url')
    ory_url = api_config.get('ory_url')

    ory_client = ORY_CLIENT(rest_url=ory_url, username=api_config.get("username"), password=api_config.get("password"))
    ory_client.request_new_ory_token()


    # Initate the OPC UA API with a fixed namespace list
    tsdata = OPC_UA(rest_url=opcua_rest_url, opcua_url=opcua_server_url, namespaces=namespace_list, ory_client=ory_client)
    variable_1 = Variables(Id='V|ZA-HE-SWS-QoS.ActivePower', Namespace=2, IdType=1)
    variables = [variable_1]
    # Live value data of trackers
    live_value = tsdata.get_historical_aggregated_values(
        start_time=(datetime.datetime.now() - datetime.timedelta(1)),
        end_time=(datetime.datetime.now()),
        pro_interval=3600000,
        agg_name="Average",
        variable_list=variables
    )
    print(live_value)


if __name__ == "__main__":
    main()