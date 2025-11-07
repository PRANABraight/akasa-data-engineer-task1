import pandas as pd
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

class CSVIngestor:
    """
    Bronze Layer: Raw CSV data ingestion for customer data
    """
    
    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        self.raw_data = None
        self.ingestion_timestamp = None
        
    def load_raw_data(self) -> pd.DataFrame:
        """
        Load raw CSV data without any transformations
        """
        try:
            if not self.file_path.exists():
                raise FileNotFoundError(f"CSV file not found: {self.file_path}")
                
            logger.info(f"Loading customer data from {self.file_path}")
            
            # Read CSV with minimal processing
            self.raw_data = pd.read_csv(
                self.file_path,
                dtype=str,
                keep_default_na=False
            )
            
            # Add bronze layer metadata
            self.ingestion_timestamp = datetime.utcnow()
            self.raw_data['_ingestion_timestamp'] = self.ingestion_timestamp
            self.raw_data['_source_file'] = str(self.file_path)
            
            logger.info(f"Loaded {len(self.raw_data)} customer records")
            return self.raw_data
            
        except Exception as e:
            logger.error(f"Error loading CSV data: {str(e)}")
            raise
            
    def validate_schema(self) -> bool:
        """
        Validate that CSV contains required customer fields
        """
        if self.raw_data is None:
            self.load_raw_data()
            
        required_columns = {'customer_id', 'customer_name', 'mobile_number', 'region'}
        actual_columns = set(self.raw_data.columns)
        
        missing_columns = required_columns - actual_columns
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return False
            
        return True
        
    def save_bronze_data(self, output_dir: str = "data/bronze") -> str:
        """
        Save raw data to bronze layer storage
        """
        if self.raw_data is None:
            self.load_raw_data()
            
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        timestamp = self.ingestion_timestamp.strftime("%Y%m%d_%H%M%S")
        filename = f"customers_bronze_{timestamp}.parquet"
        full_path = output_path / filename
        
        self.raw_data.to_parquet(full_path, index=False)
        logger.debug(f"Saved bronze customer data to {full_path}")
        
        return str(full_path)