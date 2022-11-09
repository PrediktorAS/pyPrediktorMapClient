# Import the required packeages
import datetime

# Import OPC UA functions
from pyprediktormapclient.opc_ua import OPC_UA


def main():
    namespace_list = []
    # Connection to the servers
    opcua_rest_url = "http://10.100.59.152:13373/"
    opcua_server_url = "opc.tcp://10.100.59.219:4853"

    # Initate the OPC UA API with a fixed namespace list
    tsdata = OPC_UA(rest_url=opcua_rest_url, opcua_url=opcua_server_url, namespaces=namespace_list)
    # Live value data of trackers
    live_value = tsdata.get_historical_aggregated_values(
        start_time=(datetime.datetime.now() - datetime.timedelta(1)),
        end_time=(datetime.datetime.now()),
        pro_interval=3600000,
        agg_name="Average",
        variable_list=[
            {
                "Id": "SSO.JO-GL.Signals.Weather.Albedo",
                "Namespace": 4,
                "IdType": 1
            },
            {
                "Id": "SSO.EG-AS.Signals.Weather.Albedo",
                "Namespace": 3,
                "IdType": 1
            }
        ]
    )
    print(live_value)

    write_values = tsdata.write_historical_values(
        [
            {
                "NodeId": {
                    "Id": "SSO.JO-GL.Signals.Weather.Albedo",
                    "Namespace": 4,
                    "IdType": 1
                },
                "PerformInsertReplace": 1,
                "UpdateValues": [
                    {
                        "Value": {
                            "Type": 10,
                            "Body": 1.1
                        },
                        "SourceTimestamp": "2022-11-03T12:00:00Z",
                        "StatusCode": {
                            "Code": 0
                        }
                    },
                    {
                        "Value": {
                            "Type": 10,
                            "Body": 2.1
                        },
                        "SourceTimestamp": "2022-11-03T13:00:00Z",
                        "StatusCode": {
                            "Code": 0
                        }
                    }
                ]
            }
        ]
    )

    print(write_values)

    write_wrong_values = tsdata.write_historical_values(
        [
            {
                "NodeId": {
                    "Id": "SSO.JO-GL.Signals.Weather.Albedo",
                    "Namespace": 4,
                    "IdType": 1
                },
                "PerformInsertReplace": 1,
                "UpdateValues": [
                    {
                        "Value": {
                            "Type": 10,
                            "Body": 1.1
                        },
                        "SourceTimestamp": "2022-11-03T14:00:00Z",
                        "StatusCode": {
                            "Code": 0
                        }
                    },
                    {
                        "Value": {
                            "Type": 10,
                            "Body": 2.1
                        },
                        "SourceTimestamp": "2022-11-03T13:00:00Z",
                        "StatusCode": {
                            "Code": 0
                        }
                    }
                ]
            }
        ]
    )
    print(write_wrong_values)

if __name__ == "__main__":
    main()