# Import the required packeages
import datetime

# Import OPC UA functions
from pyprediktormapclient.opc_ua import OPC_UA, Variables, WriteHistoricalVariables, Value, SubValue


def main():
    namespace_list = []
    # Connection to the servers
    opcua_rest_url = "http://10.100.59.152:13373/"
    opcua_server_url = "opc.tcp://10.100.59.219:4853"

    # Initate the OPC UA API with a fixed namespace list
    tsdata = OPC_UA(rest_url=opcua_rest_url, opcua_url=opcua_server_url, namespaces=namespace_list)
    variable_1 = Variables(Id='SSO.JO-GL.Signals.Weather.Albedo', Namespace=4, IdType=1)
    variable_2 = Variables(Id='SSO.EG-AS.Signals.Weather.Albedo', Namespace=4, IdType=1)
    variables = [variable_1, variable_2]
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