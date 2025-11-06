

## What This Program Does

Calculates 4 important business metrics (KPIs):

1. **Repeat Customers** - Finds customers who ordered more than once
2. **Monthly Trends** - Shows how many orders per month
3. **Regional Revenue** - Shows which regions make most money
4. **Top Customers** - Top 10 customers by spending (last 30 days)

## Step-by-Step Setup




### Step 1: Install Required Libraries

```bash
# Install all needed libraries
pip install -r requirements.txt
```

### Step 2: Setup Database (Optional)

If you want to use MySQL database:

1. Install MySQL from: https://dev.mysql.com/downloads/

2. Create a database:
```sql
CREATE DATABASE akasaair_analytics;
```

### Step 3. Create `.env` file with your passwords:
```
DB_HOST=localhost
DB_NAME=akasaair_analytics
DB_USER=root
DB_PASSWORD=your_password_here
```

**Important:** Keep `.env` file secret! Don't share it with anyone!

### Step 6: Run the Program

```bash
python main.py
```

## Understanding the Output

### Example Output:

```
KPI 1: REPEAT CUSTOMERS
----------------------------------------------------------------------
customer_id  customer_name    region  order_count  total_spend
CUST-001     Aarav Mehta      West    2            12749.00
```

This means:
- Aarav Mehta ordered 2 times
- Total spent: ₹12,749
- Lives in West region

## Two Ways to Process Data

### Option A: Table-Based (MySQL Database)
- **When to use:** Large datasets (1 million+ rows)
- **Pros:** Very fast for complex queries
- **Cons:** Need to install MySQL

### Option B: In-Memory (Pandas)
- **When to use:** Small datasets (< 1 million rows)
- **Pros:** Easy, no database needed
- **Cons:** Uses computer memory

**The program runs BOTH approaches automatically!**



### Problem 1: "Can't connect to database"
**Solution:** 
- Check if MySQL is running
- Check passwords in `.env` file
- The program will still work with in-memory approach!



## What Each File Does

### `main.py`
The main program with two classes:
- `TableBasedProcessor` - Uses MySQL database
- `InMemoryProcessor` - Uses Pandas (no database)

### `requirements.txt`
Lists all Python libraries needed:
```
pandas                  # For data processing
mysql-connector-python  # For database
python-dotenv           # For loading passwords safely
```

### `.env` 
Stores passwords safely:
```
DB_HOST=localhost
DB_NAME=akasaair_analytics
DB_USER=root
DB_PASSWORD=your_password
```

### `data_processing.log`
Records what happened (auto-created):
```
2025-11-06 10:30:15 - INFO - Loading customers from assets/customers.csv
2025-11-06 10:30:16 - INFO - Successfully loaded 5 customers
```

## Security Tips

**DO:**
- Keep `.env` file private
- Use parameterized queries (already done in code!)
- Check logs for errors


## Understanding the Code

### Loading CSV Data
```python
def load_customers_from_csv(file_path):
    df = pd.read_csv(file_path)  # Read file
    df = df.dropna()              # Remove empty rows
    return df                     # Return data
```

### Loading XML Data
```python
def load_orders_from_xml(file_path):
    tree = ET.parse(file_path)   # Read XML
    root = tree.getroot()         # Get root element
    # Extract data...
```

### Calculating KPI (Example)
```python
def get_repeat_customers(self):
    # Step 1: Count orders per customer
    # Step 2: Filter customers with > 1 order
    # Step 3: Return results
```



## Next Steps

Once comfortable with this, try:

1. **Add more KPIs**
   - Average order value per customer
   - Most popular products
   - Customer growth rate

2. **Add visualizations**
   ```python
   import matplotlib.pyplot as plt
   df.plot(kind='bar')
   plt.show()
   ```

3. **Automate daily runs**
   - Use Windows Task Scheduler
   - Or cron jobs on Linux

4. **Export results to Excel**
   ```python
   df.to_excel('results.xlsx', index=False)
   ```

## Checklist

Before running:
-  Python installed
-  Created project folder
-  Installed libraries (`pip install -r requirements.txt`)
-  Data files in `assets/` folder
-  (Optional) MySQL installed and `.env` created

---
PROCESSING COMPLETE!


All KPIs calculated successfully
Check 'data_processing.log' for detailed logs


---

**Made with ❤️ by Pranab**
