import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, regexp_replace
from data_analysis_system import DataAnalysisSystem


class DataAnalysisSystemTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Create a SparkSession for testing
        cls.spark = SparkSession.builder.appName(
            "test_data_analysis_system"
        ).getOrCreate()
        cls.dataset_path = "test_datasets/"
        cls.analyzer = DataAnalysisSystem(cls.dataset_path)

    @classmethod
    def tearDownClass(cls):
        # Stop the SparkSession
        cls.spark.stop()

    def test_clean_data(self):
        # Test the clean_data method
        df = self.analyzer.clean_data(
            self.dataset_path, "Test Data - Task Management System - Projects.csv"
        )
        self.assertIsNotNone(df)
        self.assertEqual(df.count(), 4)  # Assert the expected count of rows

    def test_highest_billed_client(self):
        # Test the highest_billed_client method
        input_df = self.spark.createDataFrame(
            [
                ("Client A", "$100", "2023-05-01"),
                ("Client B", "$200", "2023-05-02"),
                ("Client C", "$150", "2023-05-03"),
                ("Client A", "$300", "2023-05-04"),
            ],
            ["Customer_Name", "Amount", "Date"],
        )
        print("input")
        input_df.show()
        result_df = self.analyzer.highest_billed_client(input_df)
        # Define the expected result dataframe
        expected_df = self.spark.createDataFrame(
            [("Client A", 400.0)],
            ["Customer_Name", "Total_Amount"],
        )
        print("tests here result")
        result_df.show()
        print("expected df")
        expected_df.show()
        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_highest_roi_client(self):
        # Test the highest_roi_client method
        df_ops = self.spark.createDataFrame(
            [
                ("Client A", 100.0),
                ("Client B", 200.0),
                ("Client C", 150.0),
                ("Client A", 300.0),
            ],
            ["Customer_Name", "Amount"],
        )
        df_time = self.spark.createDataFrame(
            [
                ("Client A", 10.0),
                ("Client B", 5.0),
                ("Client C", 8.0),
                ("Client A", 15.0),
            ],
            ["Customer_Name", "Hours"],
        )

        result_df = self.analyzer.highest_roi_client(df_ops, df_time)
        expected_df = self.spark.createDataFrame(
            [
                ("Client A", 700.0, 25.0, 28.0),
            ],
            ["Customer_Name", "Total_Amount", "Total_Hours", "ROI"],
        )
        self.assertEqual(result_df.collect(), expected_df.collect())


if __name__ == "__main__":
    unittest.main()
