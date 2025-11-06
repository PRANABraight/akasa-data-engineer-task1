import pandas as pd
import xml.etree.ElementTree as ET
import logging


logger = logging.getLogger(__name__)

def load_customers_from_csv(file_path):
    """
    Load customer data from CSV file
    
    """
    try:
        logger.info(f"Loading customers from {file_path}")
        
        # Read CSV file
        df = pd.read_csv(file_path)
        
        # Clean the data
        df.columns = df.columns.str.strip()
        df = df.dropna()
        df['mobile_number'] = df['mobile_number'].astype(str).str.strip()
        df['region'] = df['region'].str.strip()
        
        logger.info(f"Successfully loaded {len(df)} customers")
        return df
        
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except Exception as e:
        logger.error(f"Error loading customers: {e}")
        raise


def load_orders_from_xml(file_path):
    """
    Load order data from XML file
    
    """
    try:
        logger.info(f"Loading orders from {file_path}")
        
        # Parse XML file
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        # Extract data from XML
        orders = []
        for order in root.findall('order'):
            orders.append({
                'order_id': order.find('order_id').text,
                'mobile_number': order.find('mobile_number').text,
                'order_date_time': order.find('order_date_time').text,
                'sku_id': order.find('sku_id').text,
                'sku_count': int(order.find('sku_count').text),
                'total_amount': float(order.find('total_amount').text)
            })
        
        # Convert to DataFrame
        df = pd.DataFrame(orders)
        
        # DATA CLEANING & NORMALIZATION
        # 5.d. Time Zone Awareness
        df['order_date_time'] = pd.to_datetime(df['order_date_time'], utc=True)
        df['order_date_time'] = df['order_date_time'].dt.tz_localize(None)
        
        # 2. Handle missing values
        df = df.dropna(subset=['order_id', 'mobile_number', 'order_date_time'])
        
        # 3. Ensure type consistency
        df['mobile_number'] = df['mobile_number'].astype(str).str.strip()
        df['order_id'] = df['order_id'].astype(str).str.strip()
        df['sku_id'] = df['sku_id'].astype(str).str.strip()
        df['sku_count'] = df['sku_count'].astype(int)
        df['total_amount'] = df['total_amount'].astype(float)
        
        # 4. Data validation
        df = df[df['sku_count'] > 0]
        df = df[df['total_amount'] > 0]
        
        logger.info(f"Successfully loaded {len(df)} order items")
        return df
        
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except Exception as e:
        logger.error(f"Error loading orders: {e}")
        raise