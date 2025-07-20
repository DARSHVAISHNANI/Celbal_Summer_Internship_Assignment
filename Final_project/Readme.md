# 🌍 Automated Data Pipelines for Country & Customer Data (Local & Azure-ready)

## 🧠 Overview

This project demonstrates a **robust, automated data pipeline solution**, originally intended for **Azure**, but implemented for **local testing** due to subscription limits. The pipeline integrates:
- REST API data extraction
- Trigger-based scheduling
- MySQL-based data transfer and validation
- Orchestrated parent-child process pipelines

Azure adaptation guidance is included for full cloud deployment.

---

## 📁 Folder Structure

```
.
├── 5 Countries Fetching and Trigger/
│   └── fetch_countries.py
│   └── <country_name>.json  (e.g., india.json, us.json, etc.)
├── country_data/
│   └── [Saved JSON files for each country]
├── Data_Creation/
│   └── generate_data.py
│   └── source_database.sql
│   └── Creating_a_database.sql
├── Full_Load/
│   └── parent_customer_pipeline.py
│   └── child_product_pipeline.py
├── Incremental_Load/
│   └── Incremental_parent_customer_pipeline.py
```

---

## 🧩 Problem Statement & Solution Approach

### ✅ 1. Fetch & Store Country Data via REST API

- **Goal:** Retrieve country data for `India`, `US`, `UK`, `China`, and `Russia` from the Rest Countries API and store them as JSON files.
- **Script:** `fetch_countries.py`

**Local Workflow (Python):**
```python
import requests, json, os
countries = ['india', 'us', 'uk', 'china', 'russia']
base_url = "https://restcountries.com/v3.1/name/"
os.makedirs("country_data", exist_ok=True)
for country in countries:
    resp = requests.get(f"{base_url}{country}")
    with open(f"country_data/{country}.json", "w") as f:
        json.dump(resp.json(), f, indent=4)
```

**Azure Alternative:**
- Use ADF “Copy Data” with REST API as the source and ADLS as the sink.
- Enable dynamic file naming using dataset parameters.

📸 Screenshots:

✅ Json Output of Countries Data
 ![Json Output of Countries Data]()

---

### 🕒 2. Schedule Triggers – Automatic Execution Twice Daily

- **Goal:** Run the country-fetching pipeline **automatically at 12:00 AM & 12:00 PM IST**.

**Local Approach:**  
Use **Windows Task Scheduler** or **cron** jobs (Linux/Mac).

**Azure Adaptation:**  
Use **ADF time-based triggers** with recurrence and IST timezone settings.

📸 Screenshots:

✅ Daily Trigger Creation
 ![Daily Trigger Creation]()


✅ Python Program Path for Execution 
 ![Python Program Path for Execution]()

---

### 🔁 3. Pipeline: Conditional DB-to-Data Lake Transfer & Child Trigger

- **Goal:** Copy `customer` data from source DB if count > 500, then conditionally run a child pipeline to copy `product` data if count > 600.

**Scripts:**
- `parent_customer_pipeline.py`
- `child_product_pipeline.py`

**Highlights:**
- Parent counts customer records and triggers child script with the count as a CLI parameter.
- Data movement uses `sqlalchemy` + `pandas`.

**Azure Implementation:**
- Use Lookup/Stored Procedure for count retrieval.
- Apply If Condition & Execute Pipeline in ADF.
- Use Azure SQL & ADLS for source/sink.

✅ Data Transfer Proof 
 ![Data Transfer Proof]()

---

### 📦 4. Parameter Passing Between Pipelines

**Local (Python):**
- Parent runs: `python child_product_pipeline.py <customer_count>`
- Child script uses `sys.argv[]` to retrieve parameter.

**Azure:**
- Define parameters in both parent & child pipelines.
- Use ADF's "Execute Pipeline" activity with parameter expressions.

---

### 🔁 5. Incremental Data Load

- **Script:** `Incremental_parent_customer_pipeline.py`
- **Goal:** Load only new or updated customer records based on `last_updated` timestamp.

**Features:**
- Full + incremental logic
- Simulates UPSERT using timestamp comparisons

**Azure Version:**
- Use Copy Data with delta/watermark columns
- Use Mapping Data Flows or Synapse SQL for merge logic

✅ Incremental Load Execution  
 ![Incremental Load Execution]()

✅ Full Load Execution  
 ![Full Load Execution]()

---

## 🗃️ Database Schema & Mock Data for Full Load

Run these SQL files to set up your databases:

- `Creating_a_database.sql` — Create source & target DBs.
- `source_database.sql` — Insert mock customer data (605+ records).

✅ Source_Database 
 ![Source_Database]()


✅ Output: Local_Database 
 ![Local_Database]()

## 🗃️ Database Schema & Mock Data for Incremental Load


✅ Source_DB 
 ![Source_Db]()


✅ Output: Local_DB 
 ![Local_Db]()

---

## 🛠️ How to Run This Project Locally

1. **Install Requirements**
   ```bash
   pip install requests pandas sqlalchemy pymysql
   ```

2. **Set Up MySQL**
   - Run `Creating_a_database.sql` and `source_database.sql` in MySQL Workbench or CLI.

3. **Fetch Country Data**
   ```bash
   python 5\ Countries\ Fetching\ and\ Trigger/fetch_countries.py
   ```

4. **Run Full Load Pipelines**
   ```bash
   python Full_Load/parent_customer_pipeline.py
   ```

5. **Run Incremental Pipeline**
   ```bash
   python Incremental_Load/Incremental_parent_customer_pipeline.py
   ```

6. **Schedule Automations**
   - Use Windows Task Scheduler / cron to automate your script runs.

---

## 🧾 Project Dependencies

- Python 3.x
- Modules:
  - `requests`
  - `pandas`
  - `sqlalchemy`
  - `pymysql`
- MySQL Server (local or remote)
- OS Scheduler (cron / Task Scheduler)

---

## ☁️ Azure Migration Guidelines

To deploy this in Azure:
| Local Component            | Azure Equivalent                          |
|---------------------------|--------------------------------------------|
| Python Scripts            | Azure Data Factory Pipelines              |
| MySQL (local)             | Azure SQL Database                        |
| Local JSON/CSV Files      | Azure Data Lake Storage (Gen2)            |
| OS Schedulers             | ADF Time Triggers                         |
| CLI Parameter Passing     | ADF Pipeline Parameters                   |
| Script-based branching    | ADF Control Flow (If Condition, Execute)  |

---

## 🎯 Key Learnings & Takeaways

- Complete end-to-end automation using REST, SQL, and conditional logic.
- Modular Python scripts simulate real-world data engineering pipelines.
- Easily adaptable to cloud-native solutions like **Azure Data Factory** and **ADLS**.
- Emphasis on **scalability**, **triggering**, **parameterization**, and **incremental data handling**.

---

## 👨‍💻 Author

**Darsh Vaishnani**
