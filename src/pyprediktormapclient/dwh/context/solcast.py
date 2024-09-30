import json
from typing import Dict, List, Union

from pydantic import validate_call

from ..idwh import IDWH


class Solcast:
    def __init__(self, dwh: IDWH) -> None:
        self.dwh = dwh

    @validate_call
    def get_plants_to_update(self) -> List:
        query = "SET NOCOUNT ON; EXEC dwetl.GetSolcastPlantsToUpdate"
        return self.dwh.fetch(query)

    @validate_call
    def upsert_forecast_data(
        self,
        plantname: str,
        solcast_forecast_data: Dict,
        forecast_type_key: Union[int, None] = None,
    ) -> List:
        solcast_forecast_data_json = json.dumps(
            {
                "results": {
                    "plantname": plantname,
                    "values": solcast_forecast_data["forecasts"],
                }
            }
        )

        query = "EXEC dwetl.UpsertSolcastForecastData ?, ?"
        return self.dwh.execute(
            query, solcast_forecast_data_json, forecast_type_key
        )
