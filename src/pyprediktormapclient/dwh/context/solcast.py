import json
from pydantic import validate_call
from typing import List, Dict, Any, Union

from ..idwh import IDWH


class Solcast:
    def __init__(self, dwh: IDWH) -> None:
        self.dwh = dwh

    @validate_call
    def get_plants_to_update(self) -> List[Any]:
        query = "SET NOCOUNT ON; EXEC dwetl.GetSolcastPlantsToUpdate"
        return self.dwh.fetch(query)

    @validate_call
    def upsert_forecast_data(
        self,
        plantname: str,
        solcast_forecast_data: Dict,
        forecast_type_key: Union[int, None] = None,
    ) -> List[Any]:
        solcast_forecast_data_json = json.dumps(
            {
                "results": {
                    "plantname": plantname,
                    "values": solcast_forecast_data["forecasts"],
                }
            }
        )
        query = f"EXEC dwetl.UpsertSolcastForecastData {solcast_forecast_data_json}, {forecast_type_key}"
        return self.dwh.execute(query)
