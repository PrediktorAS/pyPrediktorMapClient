import json
from typing import Dict, List

from pydantic import validate_call

from ..idwh import IDWH


class Plant:
    def __init__(self, dwh: IDWH) -> None:
        self.dwh = dwh

    @validate_call
    def get_optimal_tracker_angles(self, facility_name: str) -> List:
        query = (
            f"SET NOCOUNT ON; EXEC dwetl.GetOptimalTrackerAngleParameters "
            f"@FacilityName = N'{facility_name}'"
        )
        return self.dwh.fetch(query)

    @validate_call
    def upsert_optimal_tracker_angles(self, facility_data: Dict) -> List:
        facility_data_json = json.dumps(facility_data)
        facility_data_json.replace("'", '"')

        query = "EXEC dwetl.UpsertOptimalTrackerAngles @json = ?"
        return self.dwh.execute(query, facility_data_json)

    @validate_call
    def insert_log(
        self,
        plantname: str,
        ext_forecast_type_key: int,
        data_type: str,
        has_thrown_error: bool = False,
        message: str = "",
    ) -> List:
        query = "EXEC dwetl.InsertExtDataUpdateLog @plantname = ?, @extkey = ?, @DataType = ?, @Message = ?, @Result = ?"
        return self.dwh.execute(
            query,
            plantname,
            ext_forecast_type_key,
            data_type,
            message,
            "ERROR" if has_thrown_error else "OK",
        )
