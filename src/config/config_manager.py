
import os
import logging
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

logger = logging.getLogger(__name__)

class ConfigManager:
    """
    Centralized configuration management with security best practices
    """
    
    def __init__(self):
        self.base_dir = Path(__file__).parent.parent.parent
        self._validate_environment()
        
    def _validate_environment(self):
        """Validate required environment variables"""
        required_vars = ['DB_HOST', 'DB_NAME', 'DB_USER']
        
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            logger.warning(f"Missing environment variables: {missing_vars}")
            
    @property
    def database_config(self):
        """Get database configuration securely"""
        return {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '3306'),
            'database': os.getenv('DB_NAME', 'akasaair_analytics'),
            'username': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD', ''),
        }
        
    @property
    def file_paths(self):
        """Get file paths configuration"""
        return {
            'customers_csv': self.base_dir / 'assets' / 'task_DE_new_customers.csv',
            'orders_xml': self.base_dir / 'assets' / 'task_DE_new_orders.xml',
            'bronze_data': self.base_dir / 'data' / 'bronze',
            'silver_data': self.base_dir / 'data' / 'silver',
            'gold_data': self.base_dir / 'data' / 'gold',
            'logs': self.base_dir / 'logs'
        }
        
    @property
    def processing_config(self):
        """Get data processing configuration"""
        return {
            'timezone': os.getenv('TIMEZONE', 'UTC'),
            'log_level': os.getenv('LOG_LEVEL', 'INFO'),
            'data_retention_days': int(os.getenv('DATA_RETENTION_DAYS', '90')),
            'chunk_size': 10000,
            'max_workers': 4
        }
        
    @property
    def security_config(self):
        """Get security configuration"""
        return {
            'parameterized_queries': True,
            'credential_encryption': True,
            'log_sanitization': True,
            'query_timeout': 300  # 5 minutes
        }
        
    def validate_paths(self):
        """Validate that all required paths exist"""
        paths = self.file_paths
        
        # Check source files
        if not paths['customers_csv'].exists():
            raise FileNotFoundError(f"Customers CSV file not found: {paths['customers_csv']}")
        if not paths['orders_xml'].exists():
            raise FileNotFoundError(f"Orders XML file not found: {paths['orders_xml']}")
            
        # Create data directories if they don't exist
        for data_dir in [paths['bronze_data'], paths['silver_data'], paths['gold_data'], paths['logs']]:
            data_dir.mkdir(parents=True, exist_ok=True)
            
        logger.info("All paths validated successfully")
        
    def get_logging_config(self):
        """Get logging configuration"""
        return {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'standard': {
                    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                },
            },
            'handlers': {
                'file': {
                    'level': 'INFO',
                    'class': 'logging.FileHandler',
                    'filename': self.file_paths['logs'] / 'akasaair_processing.log',
                    'formatter': 'standard',
                },
                'console': {
                    'level': 'INFO',
                    'class': 'logging.StreamHandler',
                    'formatter': 'standard',
                }
            },
            'loggers': {
                '': {
                    'handlers': ['file', 'console'],
                    'level': 'INFO',
                    'propagate': True
                }
            }
        }