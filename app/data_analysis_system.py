from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, sum, regexp_replace


class DataAnalysisSystem:
    def __init__(self, path_to_datasets: str):
        """
        Initialize the DataAnalysisSystem object.
        Args:
            path_to_datasets (str): Path to the datasets directory.
        """
        self.spark = SparkSession.builder.appName("DataAnalysisSystem").getOrCreate()
        self.path_to_datasets = path_to_datasets

    def stop(self):
        """Stop the SparkSession"""
        print("Data writes complete")
        self.spark.stop()

    def clean_data(self, path: str, file_name: str) -> DataFrame:
        """
        Read the CSV files into cleaned up DataFrames.
        Args:
            path (str): Path to the CSV file.
            file_name (str): File name of the CSV file.
        Returns:
            DataFrame: Spark DataFrame.
        """
        try:
            temp_df = self.spark.read.csv(
                path + file_name,
                header=True,
                inferSchema=True,
            ).dropDuplicates()

            # Replace the $ and , in the Amount column and cast it to double, if Amount column exists
            if "Amount" in temp_df.columns:
                temp_df = temp_df.withColumn(
                    "Amount", regexp_replace("Amount", "[$,]", "").cast("double")
                )

            # Fill the lat and lon columns with 0.0 if they exist, to avoid null values.
            # NOTE: There can be a debate about the design choice of null vs placeholders.
            if "lat" in temp_df.columns:
                temp_df = temp_df.na.fill(0.0, "lat")
            if "lon" in temp_df.columns:
                temp_df = temp_df.na.fill(0.0, "lon")

            return temp_df
        except Exception as e:
            print(f"Error cleaning data for file {file_name}: {str(e)}")
            return None

    def write_to_csv(self, dataframe: DataFrame, path: str) -> None:
        """Write DataFrame to a CSV file for exploratory analysis.

        Args:
            dataframe (DataFrame): PySpark DataFrame.
            path (str): Path to write the CSV file.
        """
        try:
            dataframe.write.mode("overwrite").option("header", "true").csv(path)
        except Exception as e:
            print(f"Error writing DataFrame to CSV: {str(e)}")

    def highest_billed_client(self, df: DataFrame) -> DataFrame:
        """Find the client with the highest total amount billed.

        Args:
            df (DataFrame): PySpark DataFrame.

        Returns:
            DataFrame: PySpark DataFrame containing the single row with the highest billed client.
        """
        try:
            df_highest_billed_client = (
                df.groupBy("Customer_Name")
                .agg(sum("Amount").alias("Total_Amount"))
                .orderBy(col("Total_Amount").desc().limit(1))
            )
            print("Client with the highest billing:")
            return df_highest_billed_client.select(
                col("Customer_Name"), col("Total_Amount")
            )
        except Exception as e:
            print(f"Error finding the client with the highest billing: {str(e)}")
            return None

    def highest_roi_client(self, df_ops: DataFrame, df_time: DataFrame) -> DataFrame:
        """Find the client with the highest ROI.

        Args:
            df_ops (DataFrame): PySpark DataFrame for operations.
            df_time (DataFrame): PySpark DataFrame for time tracking.

        Returns:
            DataFrame: PySpark DataFrame containing the single row with the highest ROI.
        """
        try:
            df_time = df_time.withColumn("Hours", df_time["Hours"].cast("double"))

            df_time = df_time.groupBy("Customer_Name").agg(
                sum("Hours").alias("Total_Hours")
            )
            df_total_amount = df_ops.groupBy("Customer_Name").agg(
                sum("Amount").alias("Total_Amount")
            )

            df_ROI = df_total_amount.join(
                df_time, on="Customer_Name", how="inner"
            ).withColumn("ROI", col("Total_Amount") / col("Total_Hours"))

            print("Client with the highest ROI:")
            return df_ROI.orderBy(col("ROI").desc()).limit(1)

        except Exception as e:
            print(f"Error finding the client with the highest ROI: {str(e)}")
            return None
