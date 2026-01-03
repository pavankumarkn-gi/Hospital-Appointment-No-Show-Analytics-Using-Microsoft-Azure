# Hospital-Appointment-No-Show-Analytics-Using-Microsoft-Azure
PROJECT CODE – HOSPITAL NO-SHOW ANALYTICS (AZURE)
________________________________________
🔹 1. Azure Data Factory (ADF) – Copy Pipeline (Conceptual)
ADF uses UI, not code, so mention this in your report:
Pipeline Name: pl_copy_hospital_data
Activities:
•	Copy appointments CSV → curated container
•	Copy patients CSV → curated container
•	Copy doctors CSV → curated container
•	Copy departments CSV → curated container
Source: ADLS Gen2 (raw container)
Sink: ADLS Gen2 (curated container)
Mode: Full copy (overwrite enabled)
📌 No transformation in ADF.
________________________________________
🔹 2. Azure Databricks – Notebook Code (MAIN PART)
🔹 2.1 Spark Session (Default)
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()
________________________________________
🔹 2.2 Read Data from ADLS Gen2 (Curated Layer)
appointments = spark.read.csv(
    "abfss://curatedcontainer@hospitalnoshowstorage1.dfs.core.windows.net/export_appointments.txt",
    header=True,
    inferSchema=True
)

patients = spark.read.csv(
    "abfss://curatedcontainer@hospitalnoshowstorage1.dfs.core.windows.net/export_patients.txt",
    header=True,
    inferSchema=True
)

doctors = spark.read.csv(
    "abfss://curatedcontainer@hospitalnoshowstorage1.dfs.core.windows.net/export_doctors.txt",
    header=True,
    inferSchema=True
)

departments = spark.read.csv(
    "abfss://curatedcontainer@hospitalnoshowstorage1.dfs.core.windows.net/export_departments.txt",
    header=True,
    inferSchema=True
)
________________________________________
🔹 2.3 Data Validation
appointments.printSchema()
appointments.show(5)
________________________________________
🔹 2.4 Data Cleaning
appointments_clean = appointments.dropna(subset=["appointment_id", "status"])

appointments_clean = appointments_clean.withColumn(
    "status",
    upper(col("status"))
)
________________________________________
🔹 2.5 Overall No-Show Rate KPI
total_appointments = appointments_clean.count()

no_show_count = appointments_clean.filter(
    col("status") == "NO-SHOW"
).count()

no_show_rate = (no_show_count / total_appointments) * 100

print("Overall No-Show Rate:", round(no_show_rate, 2), "%")
________________________________________
🔹 2.6 No-Show Rate by Department
noshow_by_department = appointments_clean.groupBy("department") \
    .agg(
        count("*").alias("total_appointments"),
        sum(when(col("status") == "NO-SHOW", 1).otherwise(0)).alias("no_show_count")
    ) \
    .withColumn(
        "no_show_rate",
        round((col("no_show_count") / col("total_appointments")) * 100, 2)
    )

noshow_by_department.show()
________________________________________
🔹 2.7 No-Show Rate by Time Slot
noshow_by_time_slot = appointments_clean.groupBy("time_slot") \
    .agg(
        count("*").alias("total_appointments"),
        sum(when(col("status") == "NO-SHOW", 1).otherwise(0)).alias("no_show_count")
    ) \
    .withColumn(
        "no_show_rate",
        round((col("no_show_count") / col("total_appointments")) * 100, 2)
    )

noshow_by_time_slot.show()
________________________________________
🔹 2.8 Lead Time Impact Analysis
noshow_by_lead_time = appointments_clean.groupBy("lead_time_days") \
    .agg(
        count("*").alias("total_appointments"),
        sum(when(col("status") == "NO-SHOW", 1).otherwise(0)).alias("no_show_count")
    ) \
    .withColumn(
        "no_show_rate",
        round((col("no_show_count") / col("total_appointments")) * 100, 2)
    )

noshow_by_lead_time.show()
________________________________________
🔹 2.9 Reminder Effectiveness Analysis
noshow_by_reminder = appointments_clean.groupBy("reminder_sent") \
    .agg(
        count("*").alias("total_appointments"),
        sum(when(col("status") == "NO-SHOW", 1).otherwise(0)).alias("no_show_count")
    ) \
    .withColumn(
        "no_show_rate",
        round((col("no_show_count") / col("total_appointments")) * 100, 2)
    )

noshow_by_reminder.show()
________________________________________
🔹 2.10 Join with Patient Data (Optional Enrichment)
appointments_enriched = appointments_clean.join(
    patients,
    on="patient_id",
    how="left"
)

appointments_enriched.show(5)
________________________________________
🔹 2.11 Save Results for Power BI (CSV Export)
noshow_by_department.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .csv("dbfs:/FileStore/noshow_by_department")

noshow_by_time_slot.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .csv("dbfs:/FileStore/noshow_by_time_slot")

noshow_by_reminder.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .csv("dbfs:/FileStore/noshow_by_reminder")
⬆ Download these CSVs and load into Power BI
________________________________________
🔹 3. Power BI (No Code – Steps)
1.	Open Power BI Desktop
2.	Get Data → Text/CSV
3.	Load KPI CSV files
4.	Create:
o	KPI Card → Overall No-Show %
o	Bar Chart → No-Show by Department
o	Line Chart → No-Show by Time Slot
o	Slicer → Reminder Sent
________________________________________



