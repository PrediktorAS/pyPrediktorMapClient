# Import the required packeages
import datetime

# Import OPC UA functions
from pyprediktormapclient.opc_ua import (
    OPC_UA,
    SubValue,
    Value,
    Variables,
    WriteHistoricalVariables,
)


def main():
    namespace_list = []
    # Connection to the servers
    opcua_rest_url = "http://10.100.59.152:13373/"
    opcua_server_url = "opc.tcp://10.100.59.219:4853"

    # Initate the OPC UA API with a fixed namespace list
    tsdata = OPC_UA(
        rest_url=opcua_rest_url,
        opcua_url=opcua_server_url,
        namespaces=namespace_list,
    )
    variable_1 = Variables(
        Id="SSO.JO-GL.Signals.Weather.Albedo", Namespace=5, IdType=1
    )
    variable_2 = Variables(
        Id="SSO.EG-AS.Signals.Weather.Albedo", Namespace=3, IdType=1
    )
    variables = [variable_1, variable_2]

    live_value = tsdata.get_historical_aggregated_values(
        start_time=(datetime.datetime.now() - datetime.timedelta(1)),
        end_time=(datetime.datetime.now()),
        pro_interval=3600000,
        agg_name="Average",
        variable_list=variables,
    )
    print(live_value)

    value_1_1 = Value(
        Value=SubValue(Type=10, Body=1.1),
        SourceTimestamp=datetime.datetime.now() - datetime.timedelta(1),
    )
    value_1_2 = Value(
        Value=SubValue(Type=10, Body=2.1),
        SourceTimestamp=datetime.datetime.now(),
    )
    update_values_1 = [value_1_1, value_1_2]
    value_2_1 = Value(
        Value=SubValue(Type=10, Body=11.1),
        SourceTimestamp="2022-11-01T12:00:00",
    )
    value_2_2 = Value(
        Value=SubValue(Type=10, Body=22.1),
        SourceTimestamp="2022-11-01T13:00:00",
    )
    update_values_2 = [value_2_1, value_2_2]
    write_variable_1 = WriteHistoricalVariables(
        NodeId=variable_1, PerformInsertReplace=1, UpdateValues=update_values_1
    )
    write_variable_2 = WriteHistoricalVariables(
        NodeId=variable_2, PerformInsertReplace=1, UpdateValues=update_values_2
    )
    write_historical_data = tsdata.write_historical_values(
        [write_variable_1, write_variable_2]
    )
    print(write_historical_data)


if __name__ == "__main__":
    main()
