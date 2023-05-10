import glob
from neo4j_driver import Neo4jDriver
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import col, sum, max, regexp_replace
from pyspark.sql import SparkSession

from neo4j import GraphDatabase, basic_auth


def clean_data(path: str, file_name: str) -> DataFrame:
    temp_df = (
        spark.read.csv(
            path + file_name,
            header=True,
            inferSchema=True,
        )
        .na.drop()
        .dropDuplicates()
    )
    # Replace the $ and , in the Amount column and cast it to double, if Amount col exists
    if "Amount" in temp_df.columns:
        temp_df = temp_df.withColumn(
            "Amount", regexp_replace("Amount", "[$,]", "").cast("double")
        )
    return temp_df


def write_to_csv(dataframe: DataFrame, path: str) -> None:
    dataframe.write.mode("overwrite").option("header", "true").csv(path)


def highest_billed_client(df: DataFrame) -> DataFrame:
    df_highest_billed_client = (
        df.groupBy("Customer_Name")
        .agg(sum("Amount").alias("Total_Amount"))
        .orderBy(col("Total_Amount").desc())
        .limit(1)
    )
    return df_highest_billed_client


def highest_roi_client(df_ops, df_time) -> DataFrame:
    df_time = df_time.withColumn("Hours", df_time["Hours"].cast("double"))

    df_time = df_time.groupBy("Customer_Name").agg(sum("Hours").alias("Total_Hours"))
    df_total_amount = df_ops.groupBy("Customer_Name").agg(
        sum("Amount").alias("Total_Amount")
    )

    df_ROI = df_total_amount.join(df_time, on="Customer_Name", how="inner").withColumn(
        "ROI", col("Total_Amount") / col("Total_Hours")
    )

    return df_ROI.orderBy(col("ROI").desc()).limit(1)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("centus_demo").getOrCreate()

    # Define the columns to select
    columns_to_select = ["Customer_Name", "Invoice_Number", "Amount", "Date"]

    # List of CSV file paths
    dataset_path = "datasets/"

    # List of CSV files
    files = glob.glob("*.csv")

    # Read the CSV files into cleaned up dataframes
    df_task_management = clean_data(
        dataset_path, "Demo Data- Task Management System - Projects.csv"
    )

    df_business_ops = clean_data(
        dataset_path, "Demo Data-Business Ops_Invoice Tracking System  - Invoices.csv"
    )

    df_crm_system = clean_data(dataset_path, "Demo Data-CRM System - BD Expenses.csv")

    df_employee_time_tracking = clean_data(
        dataset_path, "Demo Data-Employee Time Tracking System - Ops Employee.csv"
    )

    # Join the dataframes
    df_hours_per_project = df_task_management.join(
        df_employee_time_tracking,
        on=["Project_Name", "Customer_Name"],
        how="inner",
    )

    # Drop the lat and lon columns, reduce visual clutter
    df_hours_per_project_no_location = df_hours_per_project.drop("lat").drop("lon")

    # Write the dataframes to CSV files for manual review
    write_to_csv(df_hours_per_project, "output/hours_per_project_per_employee")

    write_to_csv(
        df_hours_per_project_no_location,
        "output/hours_per_project_per_employee_no_location",
    )

    df_hours_per_project_no_location.show()

    print("Most billed client: ")
    highest_billed_client(df_business_ops).show()

    print("Client with highest ROI: ")
    highest_roi_client(df_business_ops, df_employee_time_tracking).show()

    neo4j_driver = Neo4jDriver(
        "bolt://3.88.131.222:7687", "neo4j", "alcoholics-conditions-tops"
    )
    # Stop the SparkSession
    spark.stop()
