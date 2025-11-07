import logging
from typing import Dict, Any
from .csv_ingestor import CSVIngestor
from .xml_ingestor import XMLIngestor

logger = logging.getLogger(__name__)

class DataLoader:
    """
    Bronze Layer: Main data loader coordinating CSV and XML ingestion
    """
    
    def __init__(self, customers_csv_path: str, orders_xml_path: str):
        self.customers_csv_path = customers_csv_path
        self.orders_xml_path = orders_xml_path
        self.csv_ingestor = CSVIngestor(customers_csv_path)
        self.xml_ingestor = XMLIngestor(orders_xml_path)
        self.bronze_data = {}
        
    def load_all_bronze_data(self) -> Dict[str, Any]:
        """
        Load all bronze layer data from both sources
        """
        try:
            logger.info("Starting bronze data ingestion")
            
            # Load customer data
            customers_raw = self.csv_ingestor.load_raw_data()
            if not self.csv_ingestor.validate_schema():
                raise ValueError("Customer CSV schema validation failed")
                
            # Load order data  
            orders_raw = self.xml_ingestor.load_raw_data()
            if not self.xml_ingestor.validate_schema():
                raise ValueError("Order XML schema validation failed")
                
            self.bronze_data = {
                'customers': customers_raw,
                'orders': orders_raw,
                'metadata': {
                    'customers_ingested': len(customers_raw),
                    'orders_ingested': len(orders_raw)
                }
            }
            
            logger.info(f"Bronze ingestion completed: {len(customers_raw)} customers, {len(orders_raw)} orders")
            return self.bronze_data
            
        except Exception as e:
            logger.error(f"Bronze data ingestion failed: {str(e)}")
            raise
            
    def save_all_bronze_data(self, output_dir: str = "data/bronze") -> Dict[str, str]:
        """
        Save all bronze data to persistent storage
        """
        try:
            if not self.bronze_data:
                self.load_all_bronze_data()
                
            customers_path = self.csv_ingestor.save_bronze_data(output_dir)
            orders_path = self.xml_ingestor.save_bronze_data(output_dir)
            
            saved_paths = {
                'customers_bronze_path': customers_path,
                'orders_bronze_path': orders_path
            }
            
            return saved_paths
            
        except Exception as e:
            logger.error(f"Failed to save bronze data: {str(e)}")
            raise