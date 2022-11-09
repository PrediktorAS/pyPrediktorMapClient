# Import OPC UA functions
from pyprediktormapclient.opc_ua import OPC_UA, Variables, WriteVariables, Value, SubValue



def main():
    namespace_list = []
    # Connection to the servers
    opcua_rest_url = "http://10.100.59.152:13373/"
    opcua_server_url = "opc.tcp://10.100.59.219:4853"

    # Initate the OPC UA API with a fixed namespace list
    tsdata = OPC_UA(rest_url=opcua_rest_url, opcua_url=opcua_server_url, namespaces=namespace_list)
    # Live value data of trackers
    # live_value = tsdata.get_values(
    #     [
    #         {
    #             'Id': 'SSO.JO-GL.Signals.Weather.Albedo',
    #             'Namespace': 4,
    #             'IdType': 1
    #         },
    #         {
    #             'Id': 'SSO.EG-AS.Signals.Weather.Albedo',
    #             'Namespace': 4,
    #             'IdType': 1
    #         }
    #     ]
    # )
    variable_1 = Variables(Id='SSO.JO-GL.Signals.Weather.Albedo', Namespace=4, IdType=1)
    variable_2 = Variables(Id='SSO.EG-AS.Signals.Weather.Albedo', Namespace=4, IdType=1)
    variables = [variable_1, variable_2]
    live_values = tsdata.get_values(variables)
    print(live_values)



    # write_values = tsdata.write_values(
    #     [
    #         {
    #             "NodeId": {
    #                 'Id': 'SSO.JO-GL.Signals.Weather.Albedo',
    #                 'Namespace': 4,
    #                 'IdType': 1
    #             },
    #             "Value": {
    #                 "Value": {
    #                     "Type": 10,
    #                     "Body": 1.2
    #                 },
    #                 "SourceTimestamp": "2022-11-03T12:00:00Z",
    #                 "StatusCode": {
    #                     "Code": 0
    #                 }
    #             }
    #         }
    #     ]
    # )
    sub_value = SubValue(Type=10, Body="3.3")
    values = Value(Value=sub_value, SourceTimestamp="2022-01-01T12:00:00Z", ServerTimestamp="2022-01-01T12:00:00Z")
    write_variables = WriteVariables(NodeId=variable_1, Value=values)
    write_values = tsdata.write_values([write_variables])
    print(write_values)

if __name__ == "__main__":
    main()