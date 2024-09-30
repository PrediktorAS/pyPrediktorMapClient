import json
from typing import Dict, List, Union

from pydantic import validate_call

from ..idwh import IDWH


class Enercast:
    def __init__(self, dwh: IDWH) -> None:
        self.dwh = dwh

    @validate_call
    def get_plants_to_update(self) -> List:
        query = "SET NOCOUNT ON; EXEC dwetl.GetEnercastPlantsToUpdate"
        return self.dwh.fetch(query)

    @validate_call
    def get_live_meter_data(self, asset_name: str) -> List:
        query = f"SET NOCOUNT ON; EXEC dwetl.GetEnercastLiveMeterData '{asset_name}'"
        return self.dwh.fetch(query)

    @validate_call
    def upsert_forecast_data(
        self,
        enercast_forecast_data: Dict,
        forecast_type_key: Union[int, None] = None,
    ) -> List:
        enercast_forecast_data_json = json.dumps(
            {"results": enercast_forecast_data}
        )

        query = "EXEC dwetl.UpsertEnercastForecastData ?, ?"
        return self.dwh.execute(
            query, enercast_forecast_data_json, forecast_type_key
        )
