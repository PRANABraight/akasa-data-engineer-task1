import os
from dotenv import load_dotenv
import logging
import pandas as pd
from datetime import datetime



from src import load_data
from src.in_memory_approach import InMemoryProcessor

from src.db_approach import get_db_engine, setup_database, get_sql_kpis


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()



def main():
    """Main function to run the data processing"""
    
    print("\n" + "="*70)
    print("CUSTOMER & ORDER ANALYTICS - DATA PROCESSING")
    print("="*70 + "\n")
    
    # File paths
    customers_file = 'assets/task_DE_new_customers.csv'
    orders_file = 'assets/task_DE_new_orders.xml'
    
    try:
        # ====================================================================
        # STEP 1: LOAD DATA
        # ====================================================================
        print("STEP 1: Loading data files...")
        print("-" * 70)
        
        customers_df = load_data.load_customers_from_csv(customers_file)
        orders_df = load_data.load_orders_from_xml(orders_file)
        
        print(f" Loaded {len(customers_df)} customers")
        print(f" Loaded {len(orders_df)} order items\n")
        
        # ====================================================================
        # APPROACH A: TABLE-BASED (MYSQL)
        # ====================================================================
        print("\n" + "="*70)
        print("APPROACH A: TABLE-BASED (USING MYSQL DATABASE)")
        print("="*70 + "\n")
        
        if os.getenv('DB_PASSWORD'):
            print("\nStep 1: Connecting to database...")
            db_engine = get_db_engine()
            
            if db_engine:
                print("\nStep 2: Setting up database and loading data...")
                
                
                setup_database(db_engine, customers_df, orders_df)
                
                print("\nStep 3: Calculating KPIs using SQL...\n")
                
                # Get all KPIs as a dictionary
                sql_kpis = get_sql_kpis(db_engine)
                
                # KPI 1
                print("KPI 1: REPEAT CUSTOMERS")
                print("-" * 70)
                print(sql_kpis['repeat_customers'].to_string(index=False))
                
                # KPI 2
                print("\n\nKPI 2: MONTHLY ORDER TRENDS")
                print("-" * 70)
                print(sql_kpis['monthly_trends']) # Already indexed
                
                # KPI 3
                print("\n\nKPI 3: REGIONAL REVENUE")
                print("-" * 70)
                print(sql_kpis['regional_revenue'].to_string(index=False))
                
                # KPI 4
                print("\n\nKPI 4: TOP CUSTOMERS (LAST 30 DAYS)")
                print("-" * 70)
                if not sql_kpis['top_spenders'].empty:
                    print(sql_kpis['top_spenders'].to_string(index=False))
                else:
                    print("No customers found in last 30 days")
                
                db_engine.dispose()
                logger.info(" Database connection closed")
            else:
                print("Could not create database engine. Check logs.")
        else:
            print("Database credentials not found. Skipping table-based approach.")
            print("  Add DB_PASSWORD to .env file to use this approach.")
        
        # ============================
        # APPROACH B: IN-MEMORY (PANDAS)
        # ============================
        print("\n\n" + "=" * 70)
        print("APPROACH B: IN-MEMORY (USING PANDAS)")
        print("=" * 70 + "\n")

        print("Calculating KPIs using Pandas...")
        try:
            # Already imported at the top: from src.in_memory_approach import InMemoryProcessor
            processor = InMemoryProcessor(
                customers_df=customers_df,
                orders_df=orders_df
            )

            # KPI 1
            k1_pd = processor.get_repeat_customers()
            print("KPI 1: REPEAT CUSTOMERS")
            print("-" * 70)
            print(k1_pd.to_string(index=False))

            # KPI 2
            k2_pd = processor.get_monthly_trends()
            print("\n\nKPI 2: MONTHLY ORDER TRENDS")
            print("-" * 70)
            print(k2_pd.to_string(index=False))

            # KPI 3
            k3_pd = processor.get_regional_revenue()
            print("\n\nKPI 3: REGIONAL REVENUE")
            print("-" * 70)
            print(k3_pd.to_string(index=False))

            # KPI 4 
            k4_pd = processor.get_top_customers_last_30_days()
            print("\n\nKPI 4: TOP CUSTOMERS (LAST 30 DAYS)")
            print("-" * 70)
            if not k4_pd.empty:
                print(k4_pd.to_string(index=False))
            else:
                print("No customers found in last 30 days")

        except Exception as ex:
            logger.exception("Error running Pandas approach")
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - ERROR - {ex}")

        # ====================================================================
        # SUMMARY
        # ====================================================================
        print("\n\n" + "="*70)
        print("PROCESSING COMPLETE!")
        print("="*70)
        print("\n All KPIs calculated successfully")
        print(" Check 'data_processing.log' for detailed logs\n")
        
    except Exception as e:
        logger.error(f"Error in main program: {e}")
        print(f"\nError occurred: {e}")
        print("Check 'data_processing.log' for details\n")


if __name__ == "__main__":
    main()
    