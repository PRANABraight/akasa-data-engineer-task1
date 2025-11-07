
import pandas as pd
import logging
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

class DataValidator:
    """
    Silver Layer: Comprehensive data validation and quality checks
    """
    
    def __init__(self):
        self.validation_results = {}
        
    def validate_customers_data(self, cleaned_customers: pd.DataFrame) -> dict:
        """
        Comprehensive validation of cleaned customer data
        """
        validation_result = {
            'timestamp': datetime.utcnow(),
            'dataset': 'customers',
            'total_records': len(cleaned_customers),
            'checks': {},
            'issues': [],
            'overall_status': 'PASS'
        }
        
        try:
            # Check 1: Data completeness
            required_fields = ['customer_id', 'customer_name', 'mobile_number', 'region']
            for field in required_fields:
                null_count = cleaned_customers[field].isnull().sum()
                validation_result['checks'][f'{field}_completeness'] = {
                    'status': 'PASS' if null_count == 0 else 'FAIL',
                    'null_count': null_count
                }
                if null_count > 0:
                    validation_result['issues'].append(f"{field} has {null_count} null values")
            
            # Check 2: Data uniqueness
            duplicate_customers = cleaned_customers['customer_id'].duplicated().sum()
            validation_result['checks']['customer_id_uniqueness'] = {
                'status': 'PASS' if duplicate_customers == 0 else 'FAIL',
                'duplicate_count': duplicate_customers
            }
            if duplicate_customers > 0:
                validation_result['issues'].append(f"Found {duplicate_customers} duplicate customer IDs")
            
            # Check 3: Mobile number format
            valid_mobile_format = cleaned_customers['mobile_number'].str.match(r'^\d{10}$').sum()
            validation_result['checks']['mobile_format'] = {
                'status': 'PASS' if valid_mobile_format == len(cleaned_customers) else 'FAIL',
                'valid_count': valid_mobile_format,
                'invalid_count': len(cleaned_customers) - valid_mobile_format
            }
            
            # Check 4: Region validation
            valid_regions = ['North', 'South', 'East', 'West', 'Unknown']
            invalid_regions = ~cleaned_customers['region'].isin(valid_regions)
            invalid_region_count = invalid_regions.sum()
            validation_result['checks']['region_validation'] = {
                'status': 'PASS' if invalid_region_count == 0 else 'FAIL',
                'invalid_count': invalid_region_count
            }
            
            # Overall status
            failed_checks = [check for check in validation_result['checks'].values() if check['status'] == 'FAIL']
            if failed_checks:
                validation_result['overall_status'] = 'FAIL'
                
            self.validation_results['customers'] = validation_result
            self._log_validation_result(validation_result)
            
            return validation_result
            
        except Exception as e:
            logger.error(f"Customer validation error: {e}")
            raise
            
    def validate_orders_data(self, cleaned_orders: pd.DataFrame) -> dict:
        """
        Comprehensive validation of cleaned order data
        """
        validation_result = {
            'timestamp': datetime.utcnow(),
            'dataset': 'orders',
            'total_records': len(cleaned_orders),
            'checks': {},
            'issues': [],
            'overall_status': 'PASS'
        }
        
        try:
            # Check 1: Data completeness
            required_fields = ['order_id', 'mobile_number', 'order_date_time', 'total_amount']
            for field in required_fields:
                null_count = cleaned_orders[field].isnull().sum()
                validation_result['checks'][f'{field}_completeness'] = {
                    'status': 'PASS' if null_count == 0 else 'FAIL',
                    'null_count': null_count
                }
                if null_count > 0:
                    validation_result['issues'].append(f"{field} has {null_count} null values")
            
            # Check 2: Positive values
            negative_amounts = (cleaned_orders['total_amount'] <= 0).sum()
            validation_result['checks']['positive_amounts'] = {
                'status': 'PASS' if negative_amounts == 0 else 'FAIL',
                'negative_count': negative_amounts
            }
            
            negative_counts = (cleaned_orders['sku_count'] <= 0).sum()
            validation_result['checks']['positive_counts'] = {
                'status': 'PASS' if negative_counts == 0 else 'FAIL',
                'negative_count': negative_counts
            }
            
            # Check 3: Date validation
            current_date = pd.Timestamp.now(tz='UTC')
            future_orders = (cleaned_orders['order_date_time'] > current_date).sum()
            validation_result['checks']['date_validation'] = {
                'status': 'PASS' if future_orders == 0 else 'FAIL',
                'future_dates_count': future_orders
            }
            
            # Check 4: Data consistency
            # Ensure total_amount makes sense for given sku_count
            avg_item_price = cleaned_orders['total_amount'] / cleaned_orders['sku_count']
            unreasonable_prices = ((avg_item_price < 0.01) | (avg_item_price > 10000)).sum()
            validation_result['checks']['price_consistency'] = {
                'status': 'PASS' if unreasonable_prices == 0 else 'WARN',
                'unreasonable_count': unreasonable_prices
            }
            
            # Overall status
            failed_checks = [check for check in validation_result['checks'].values() if check['status'] == 'FAIL']
            if failed_checks:
                validation_result['overall_status'] = 'FAIL'
                
            self.validation_results['orders'] = validation_result
            self._log_validation_result(validation_result)
            
            return validation_result
            
        except Exception as e:
            logger.error(f"Order validation error: {e}")
            raise
            
    def _log_validation_result(self, validation_result: dict):
        """Log validation results"""
        dataset = validation_result['dataset']
        status = validation_result['overall_status']
        total_checks = len(validation_result['checks'])
        failed_checks = len([c for c in validation_result['checks'].values() if c['status'] == 'FAIL'])
        
        logger.info(f"Validation {status} for {dataset}: {total_checks - failed_checks}/{total_checks} checks passed")
        
    def get_validation_summary(self) -> dict:
        """Get summary of all validation results"""
        summary = {
            'timestamp': datetime.utcnow(),
            'overall_status': 'PASS',
            'datasets': {}
        }
        
        for dataset, result in self.validation_results.items():
            summary['datasets'][dataset] = {
                'status': result['overall_status'],
                'total_records': result['total_records'],
                'checks_passed': len([c for c in result['checks'].values() if c['status'] == 'PASS']),
                'total_checks': len(result['checks']),
                'issues_count': len(result['issues'])
            }
            
            if result['overall_status'] == 'FAIL':
                summary['overall_status'] = 'FAIL'
                
        return summary