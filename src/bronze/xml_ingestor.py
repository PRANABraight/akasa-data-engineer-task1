import pandas as pd
import xml.etree.ElementTree as ET
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

class XMLIngestor:
    """
    Bronze Layer: Raw XML data ingestion for order data
    """
    
    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        self.raw_data = None
        self.ingestion_timestamp = None
        
    def load_raw_data(self) -> pd.DataFrame:
        """
        Load raw XML data maintaining original structure
        """
        try:
            if not self.file_path.exists():
                raise FileNotFoundError(f"XML file not found: {self.file_path}")
                
            logger.info(f"Loading order data from {self.file_path}")
            
            # Parse XML file
            tree = ET.parse(self.file_path)
            root = tree.getroot()
            
            # Extract all data maintaining raw structure
            orders = []
            for order_idx, order in enumerate(root.findall('order')):
                order_data = {'_xml_order_index': order_idx}
                
                # Extract all elements preserving original values
                for element in order:
                    order_data[element.tag] = element.text
                    
                orders.append(order_data)
            
            self.raw_data = pd.DataFrame(orders)
            
            # Add bronze layer metadata
            self.ingestion_timestamp = datetime.utcnow()
            self.raw_data['_ingestion_timestamp'] = self.ingestion_timestamp
            self.raw_data['_source_file'] = str(self.file_path)
            
            logger.info(f"Loaded {len(self.raw_data)} order records")
            return self.raw_data
            
        except Exception as e:
            logger.error(f"Error loading XML data: {str(e)}")
            raise
            
    def validate_schema(self) -> bool:
        """
        Validate that XML contains required order fields
        """
        if self.raw_data is None:
            self.load_raw_data()
            
        required_columns = {'order_id', 'mobile_number', 'order_date_time', 'sku_id', 'sku_count', 'total_amount'}
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
        filename = f"orders_bronze_{timestamp}.parquet"
        full_path = output_path / filename
        
        self.raw_data.to_parquet(full_path, index=False)
        logger.debug(f"Saved bronze order data to {full_path}")
        
        return str(full_path)