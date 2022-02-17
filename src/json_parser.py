
import json
import logging
import pandas as pd
from typing import List, Dict, Any

class JsonParser:
    def __init__(self, data_path_name: str):
        self.data_path_name = data_path_name
        
    def extract_relevant_fields(self, data: Dict[Any, Any]) -> pd.DataFrame:
    
        """
        Contains logic to extract relevant field from the data and generate a pd.DataFrame
        """

        parsed_data = []
        n_entries = int(data["CVE_data_numberOfCVEs"])
        
        for entry in data["CVE_Items"]:

            base_metrics_v3 = dict()
            if "baseMetricV3" in entry["impact"]:
                base_metrics_v3 = entry["impact"]["baseMetricV3"]["cvssV3"]
                base_metrics_v3 = {f"{k}_v3": v for k, v in base_metrics_v3.items()}

            base_metrics_v2 = dict()
            if "baseMetricV2" in entry["impact"]:
                base_metrics_v2 = entry["impact"]["baseMetricV2"]["cvssV2"]
                base_metrics_v2 = {f"{k}_v2": v for k, v in base_metrics_v2.items()}

            selected_entries = {
                "cve_id": entry["cve"]["CVE_data_meta"]["ID"],
                "description": entry["cve"]["description"]["description_data"][0]["value"],
                "published_date": entry["publishedDate"],
                "last_modified_date": entry["lastModifiedDate"]
            }

            selected_entries.update(base_metrics_v3)
            selected_entries.update(base_metrics_v2)

            parsed_data.append(selected_entries)
            
        if len(parsed_data) != n_entries:
            logging.warning(f"The number of entries doesn't match the extracted entries. n_entries: {n_entries} != {len(parsed_data)}")

        return pd.DataFrame.from_records(parsed_data) 
        
        
    def df_from_json(self, filename: str) -> pd.DataFrame:
        """
        Loads a json file, extracts relevant fields and converts to pd.DataFrame
        """

        f = open(f'{self.data_path_name}/{filename}', "r")
        data_dict = json.loads(f.read())
        f.close()

        data_df = self.extract_relevant_fields(data_dict)
        
        return data_df
        
    def generate_dataframe(self, filenames: List[str]) -> pd.DataFrame:
        """
        Concatenates several yearly dataframes
        """
        
        dataframes_array = []
        
        for filename in filenames:
            logging.info(f"Extracting data from: {filename}")
            yearly_df = self.df_from_json(filename)
            dataframes_array.append(yearly_df)
            logging.info(f"Finished extracting data from: {filename}")
            
        logging.info("Finished extract data")
        return pd.concat(dataframes_array, ignore_index = True)
