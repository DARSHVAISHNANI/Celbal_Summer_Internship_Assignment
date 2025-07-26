# NYC Taxi Data Analysis using PySpark

## 🔍 Project Summary
This project loads and analyzes January 2018 and January 2020 NYC Yellow Taxi data using PySpark in Google Colab. The combined data is transformed to extract business insights like revenue, vendor performance, and location-based metrics.

## 📊 Datasets Used
- [yellow_tripdata_2018-01.parquet]
- [yellow_tripdata_2020-01.parquet]

## ✅ Tasks Completed
- Loaded Parquet files into Spark DataFrames
- Merged and enriched data (Revenue column)
- Performed 7 PySpark queries
- Exported final results to Parquet

## 📁 Folder Structure



## 🧪 Tools Used
- Google Colab
- Apache Spark (via PySpark)
- Parquet format


## Screenshots Description
Query No.	Task Description	Screenshot Name (suggested)

Query 1	  Add Revenue column (sum of fare-related columns)	query1_revenue_column.png

Query 2	Count of total passengers by pickup area (PULocationID)	query2_passenger_count_by_area.png

Query 3	Average fare and earnings by VendorID	query3_avg_earning_by_vendor.png

Query 4	Moving count of payments by each payment_type	query4_payment_mode_counts.png

Query 5	Top 2 vendors by earnings per day with total passengers & distance	query5_top_2_vendors_by_day.png
Query 6	Most passengers between two locations (PULocationID → DOLocationID)	query6_top_passenger_route.png
Query 7	Top pickup locations with most passengers in last 5 or 10 seconds (advanced)	query7_top_pickups_last_10s.png