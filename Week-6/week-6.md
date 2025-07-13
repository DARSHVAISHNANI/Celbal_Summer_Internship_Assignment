🚀 Week 6 Project: Advanced Data Engineering – Hybrid ETL, SFTP Integration & Trigger Automation
🧩 Project Synopsis
In this module of the internship, I implemented practical solutions that reflect real-world data engineering workflows. Using Azure Data Factory (ADF), I designed and deployed hybrid pipelines to connect on-premises systems and external sources to Azure-based services. I also added automation and incremental load capabilities to simulate production-grade data orchestration.

👨‍💻 Project completed as part of Celebal Summer Internship (CSI)
This task was instrumental in strengthening my hands-on understanding of cloud-scale data workflows.

🧰 Tech Stack Overview
Category	Tools/Services
Orchestration	Azure Data Factory (ADF)
Cloud Storage	Azure SQL Database, Azure Blob Storage
On-Premise Runtime	Self-Hosted Integration Runtime (SHIR)
External Source	SFTP Server (test.rebex.net)
Automation Features	Triggers, Retry Logic

🔧 Tasks & Implementation Details
1️⃣ Hybrid Data Movement (On-Prem SQL Server → Azure SQL DB)
Goal: Migrate all tables from a local SQL Server instance to Azure SQL using SHIR.

Approach:

Used a Windows system (friend’s laptop) to install SQL Server with the Northwind sample DB, since I work on macOS.

Set up and registered Self-Hosted IR in Azure.

Built pipeline: PL_Full_OnPrem_Replication

Dynamically lists all table names via Lookup

Applies ForEach loop to copy all tables using Copy data

📸 Screenshots:

✅ SHIR connected to Azure
 ![SHIR](https://github.com/DARSHVAISHNANI/Celbal_Summer_Internship_Assignment/blob/main/Week-6/screenshots/Self-hosted%20node%20is%20connected%20to%20the%20cloud%20service.jpeg)

✅ SHIR visible in Integration Runtimes
 ![SHIR](https://github.com/DARSHVAISHNANI/Celbal_Summer_Internship_Assignment/blob/main/Week-6/screenshots/Integration%20runtimes.png)
  

✅ Dynamic pipeline executed and copied all tables
 ![SHIR]([https://github.com/DARSHVAISHNANI/Celbal_Summer_Internship_Assignment/blob/main/Week-6/screenshots/Cloud_Destination_DB%20.png?raw=true](https://github.com/DARSHVAISHNANI/Celbal_Summer_Internship_Assignment/blob/main/Week-6/screenshots/PL_Full_OnPrem_Replication%20pipeline%20copied%20all%20tables.png))
  

✅ Tables confirmed in Azure SQL Database
 ![SHIR](https://github.com/DARSHVAISHNANI/Celbal_Summer_Internship_Assignment/blob/main/Week-6/screenshots/Cloud_Destination_DB%20.png)
  

✅ Protocol configuration for SQL Server
 ![SHIR](https://github.com/DARSHVAISHNANI/Celbal_Summer_Internship_Assignment/blob/main/Week-6/screenshots/Protocols%20for%20SQL%20Server.jpeg)

2️⃣ External Data Extraction from SFTP
Goal: Download file from an external SFTP server and upload it to Azure Blob Storage.

Approach:

Connected to the public SFTP server test.rebex.net using Binary format.

Developed pipeline: PL_SFTP_File_Copy

Downloads readme.txt

Uploads to Azure Blob Storage container data-output

📸 Screenshots:

✅ Successful connection to SFTP server
 ![SHIR](https://github.com/DARSHVAISHNANI/Celbal_Summer_Internship_Assignment/blob/main/Week-6/screenshots/SFTP%20Linked%20Service%20in%20ADF.png)

✅ File copied to Blob container
 ![SHIR](https://github.com/DARSHVAISHNANI/Celbal_Summer_Internship_Assignment/blob/main/Week-6/screenshots/Azure%20Blob%20Storage%20account%20(container)%20.png)

✅ File visible in Azure Blob Storage
 ![SHIR](https://github.com/DARSHVAISHNANI/Celbal_Summer_Internship_Assignment/blob/main/Week-6/screenshots/Created%20a%20Container.png)

3️⃣ Incremental Load Using Watermark Pattern
Goal: Transfer only newly added or updated rows from Orders table.

Components Used:

Lookup for previous and current watermark values

Copy data with dynamic SQL to copy delta records

Stored Procedure to update watermark after successful copy

SQL Code:

sql
Copy
Edit
-- Watermark table
CREATE TABLE dbo.WatermarkTable (
    TableName NVARCHAR(255) PRIMARY KEY,
    WatermarkValue DATETIME
);

-- Insert initial watermark
INSERT INTO dbo.WatermarkTable VALUES ('Orders', '1990-01-01');
sql
Copy
Edit
-- Procedure to update watermark
CREATE PROCEDURE dbo.sp_UpdateWatermark
    @NewWatermarkValue DATETIME,
    @TableName NVARCHAR(255)
AS
BEGIN
    UPDATE dbo.WatermarkTable
    SET WatermarkValue = @NewWatermarkValue
    WHERE TableName = @TableName;
END
sql
Copy
Edit
-- Dynamic SQL inside Copy Activity
@concat(
  'SELECT * FROM dbo.Orders WHERE OrderDate > ''',
  activity('GetOldWatermark').output.firstRow.WatermarkValue,
  ''' AND OrderDate <= ''',
  activity('GetNewWatermark').output.firstRow.NewWatermark,
  ''''
)
📸 Screenshots:

✅ Watermark table created successfully
 ![SHIR](https://github.com/DARSHVAISHNANI/Celbal_Summer_Internship_Assignment/blob/main/Week-6/screenshots/Watermark%20table%20created%20successfully.png)

✅ Incremental load pipeline configured
 ![SHIR](https://github.com/DARSHVAISHNANI/Celbal_Summer_Internship_Assignment/blob/main/Week-6/screenshots/PL_Incremental_Load_Orders.png)

4️⃣ Pipeline Resilience & Monthly Trigger Setup
Goal: Add reliability through retry policies and schedule pipeline execution monthly.

Implementation:

Retry Logic: Set to 3 retries, with 30 seconds delay

Monthly Trigger: Configured to run on last Saturday of every month at 7:00 AM

📸 Screenshots:

✅ Monthly trigger set correctly
 ![SHIR](https://github.com/DARSHVAISHNANI/Celbal_Summer_Internship_Assignment/blob/main/Week-6/screenshots/TR_Monthly_Last_Saturday.png)

✅ Trigger successfully executed the pipeline
 ![SHIR](https://github.com/DARSHVAISHNANI/Celbal_Summer_Internship_Assignment/blob/main/Week-6/screenshots/Master%20PL.png)

✅ Retry logic handled transient failures
 ![SHIR](https://github.com/DARSHVAISHNANI/Celbal_Summer_Internship_Assignment/blob/main/Week-6/screenshots/Retrieving%20data.%20Wait%20a%20few%20seconds%20and%20try%20to%20cut%20or%20copy%20again.png)

📊 Completion Summary
Feature	Status
On-Prem → Azure Data Transfer	✅ Done
SFTP Integration to Blob Storage	✅ Done
Incremental Load with Watermark Logic	✅ Done
Trigger and Retry Automation	✅ Done

🧠 Challenges & Solutions
MacBook Limitations: Couldn’t install SQL Server locally
→ Used a Windows machine and Azure VM for implementation

SHIR Setup Issues: Had to regenerate keys and troubleshoot firewall configs

Dynamic Copy Logic: Required debugging of expressions and JSON inputs

Special Table Names: Encountered issues with brackets/spaces in table names

📚 Learnings Gained
Developed hybrid data pipelines connecting on-premises and cloud

Mastered dynamic pipeline design using Lookup & ForEach

Applied watermark logic for delta loading

Integrated SFTP workflows with Azure Storage

Used retry logic and scheduled triggers for fault-tolerance
