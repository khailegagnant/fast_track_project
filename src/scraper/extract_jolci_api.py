

import os
import json
import logging
import time
from typing import Dict, Any, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load variables from .env
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("f1_ingestion")

# Circuits don't change much; we pull them globally once to save API calls
GLOBAL_ENDPOINTS = {
    "circuits": "circuits.json"
}

# These are pulled per season
SEASON_ENDPOINTS = {
    "constructors": "{season}/constructors.json",
    "drivers": "{season}/drivers.json",
    "races": "{season}/races.json",
    "results": "{season}/results.json",
    "qualifying": "{season}/qualifying.json",
    "driver_standings": "{season}/driverstandings.json",
    "constructor_standings": "{season}/constructorstandings.json"
}

class F1DataExtractor:
    def __init__(self, s3_bucket: str, s3_prefix: str):
        self.base_url = "https://api.jolpi.ca/ergast/f1"
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.session = self._build_retry_session()
        self.s3_client = boto3.client("s3")
        self.page_limit = 100 # Ergast max limit is 100

    def _build_retry_session(self) -> requests.Session:
        """Handles intermittent network failures and 5xx errors."""
        session = requests.Session()
        retry_strategy = Retry(
            total=5, 
            backoff_factor=2, 
            status_forcelist=[429, 500, 502, 503, 504], 
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _merge_pages(self, base_data: Dict[str, Any], new_data: Dict[str, Any]) -> Dict[str, Any]:
        """Dynamically appends paginated arrays (like Results or Drivers)."""
        mr_data = new_data.get("MRData", {})
        table_key = next((k for k in mr_data.keys() if k.endswith("Table")), None)
        if not table_key: return base_data
            
        table = mr_data[table_key]
        array_key = next((k for k in table.keys() if isinstance(table[k], list)), None)
        
        if array_key:
            base_data["MRData"][table_key][array_key].extend(table[array_key])
            
        return base_data

    def fetch_endpoint(self, path: str) -> Optional[Dict[str, Any]]:
        """Handles pagination and strictly enforces Jolpica rate limits."""
        url = f"{self.base_url}/{path}"
        offset = 0
        merged_data = None
        total_records = 0

        while True:
            params = {"limit": self.page_limit, "offset": offset}
            
            # API RATE LIMIT PROTECTION: Sleep to respect 4 requests/second limit
            time.sleep(0.35) 
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            mr_data = data.get("MRData", {})
            total = int(mr_data.get("total", 0))
            
            if not merged_data:
                merged_data = data
            else:
                merged_data = self._merge_pages(merged_data, data)
            
            current_count = int(mr_data.get("limit", 0)) + int(mr_data.get("offset", 0))
            total_records = total
            
            if current_count >= total:
                break
            offset += self.page_limit

        logger.info(f"Extracted {total_records} records from {path}.")
        return merged_data

    def upload_to_s3(self, data: Dict[str, Any], endpoint_name: str, partition: str):
        """Uploads JSON to S3 using Hive partitioning (e.g., season=2023)"""
        s3_key = f"{self.s3_prefix}/endpoint={endpoint_name}/season={partition}/data.json"
        
        try:
            payload_bytes = json.dumps(data).encode("utf-8")
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=payload_bytes,
                ContentType="application/json"
            )
            logger.info(f"Successfully landed: s3://{self.s3_bucket}/{s3_key}")
        except ClientError as e:
            logger.error(f"S3 Upload Failed for {s3_key}: {e}")
            raise

def main():
    # Load env variables safely
    S3_BUCKET = os.getenv("S3_BRONZE_BUCKET", "my-f1-capstone-lake")
    S3_PREFIX = os.getenv("S3_PREFIX", "bronze/f1")
    
    # Parse the comma-separated seasons list
    seasons_str = os.getenv("F1_SEASONS", "2022,2023,2024,2025,2026")
    seasons = [s.strip() for s in seasons_str.split(",")]

    extractor = F1DataExtractor(s3_bucket=S3_BUCKET, s3_prefix=S3_PREFIX)

    # 1. Fetch Global Endpoints
    logger.info("=== FETCHING GLOBAL DATA ===")
    for name, path in GLOBAL_ENDPOINTS.items():
        data = extractor.fetch_endpoint(path)
        if data:
            extractor.upload_to_s3(data, endpoint_name=name, partition="global")

    # 2. Fetch Season-Specific Endpoints
    logger.info(f"=== FETCHING SEASON DATA: {seasons} ===")
    for season in seasons:
        logger.info(f"--- Processing Season {season} ---")
        for name, path_template in SEASON_ENDPOINTS.items():
            path = path_template.format(season=season)
            data = extractor.fetch_endpoint(path)
            if data:
                extractor.upload_to_s3(data, endpoint_name=name, partition=season)
                
    logger.info("All Bronze extraction completed successfully.")

if __name__ == "__main__":
    main()