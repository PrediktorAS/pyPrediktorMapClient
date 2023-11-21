import json
from pydantic import validate_call
from typing import List, Dict, Any

from ..idwh import IDWH


class Plant:
    def __init__(self, dwh: IDWH) -> None:
        self.dwh = dwh

    @validate_call
    def get_optimal_tracker_angles(self, facility_name: str) -> List[Any]:
        query = (
            f"SET NOCOUNT ON; EXEC dwetl.GetOptimalTrackerAngleParameters "
            + f"@FacilityName = N'{facility_name}'"
        )
        return self.dwh.fetch(query)

    @validate_call
    def upsert_optimal_tracker_angles(self, facility_data: Dict) -> List[Any]:
        facility_data_json = json.dumps(facility_data)
        facility_data_json.replace("'", '"')
        query = f"EXEC dwetl.UpsertOptimalTrackerAngles @json = {facility_data_json}"
        return self.dwh.execute(query)

    @validate_call
    def insert_log(
        self,
        plantname: str,
        ext_forecast_type_key: int,
        data_type: str,
        has_thrown_error: bool = False,
        message: str = "",
    ) -> List[Any]:
        result = "ERROR" if has_thrown_error else "OK"
        query = (
            f"EXEC dwetl.InsertExtDataUpdateLog "
            + f"@plantname = {plantname}, "
            + f"@extkey = {ext_forecast_type_key}, "
            + f"@DataType = {data_type}, "
            + f"@Message = {message}, "
            + f"@Result = {result}"
        )
        return self.dwh.execute(query)
