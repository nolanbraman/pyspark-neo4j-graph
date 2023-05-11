from neo4j_driver import Neo4jDriver
from data_analysis import DataAnalysisSystem


if __name__ == "__main__":
    # Path to dir of csv datasets
    dataset_path = "datasets/"
    analyzer = DataAnalysisSystem(dataset_path)
    neo = Neo4jDriver("bolt://54.172.30.170:7687", "neo4j", "boot-issue-volumes")

    # Read the CSV files into cleaned up dataframes
    df_task_management = analyzer.clean_data(
        dataset_path, "Demo Data- Task Management System - Projects.csv"
    )

    if df_task_management is not None:
        df_task_management.show()

    df_business_ops = analyzer.clean_data(
        dataset_path, "Demo Data-Business Ops_Invoice Tracking System  - Invoices.csv"
    )

    df_crm_system = analyzer.clean_data(
        dataset_path, "Demo Data-CRM System - BD Expenses.csv"
    )

    df_employee_time_tracking = analyzer.clean_data(
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

    # Write some of the dataframes to CSV files for manual review
    if df_task_management is not None:
        analyzer.write_to_csv(df_task_management, "output/task_management")
    if df_hours_per_project is not None:
        analyzer.write_to_csv(
            df_hours_per_project, "output/hours_per_project_per_employee"
        )
    if df_hours_per_project_no_location is not None:
        analyzer.write_to_csv(
            df_hours_per_project_no_location,
            "output/hours_per_project_per_employee_no_location",
        )

    if df_hours_per_project_no_location is not None:
        df_hours_per_project_no_location.show()

    df_billed_client = analyzer.highest_billed_client(df_business_ops)
    if df_billed_client is not None:
        df_billed_client.show()

    df_highest_roi = analyzer.highest_roi_client(
        df_business_ops, df_employee_time_tracking
    )
    if df_highest_roi is not None:
        df_highest_roi.show()

    # Wipe the data from the database from previous runs.
    neo.wipe_data()

    # Add the data to the neo4j database
    for row in df_task_management.collect():
        neo.add_customer(
            row["Customer_Name"], row["Project_Name"], row["lat"], row["lon"]
        )

    for row in df_business_ops.collect():
        neo.add_business_ops(
            customer_name=row["Customer_Name"],
            invoice_number=row["Invoice_Number"],
            invoice_amount=row["Amount"],
            invoice_date=row["Date"],
        )

    for row in df_employee_time_tracking.collect():
        neo.add_employees(
            employee_name=row["Employee_Name"],
            project_name=row["Project_Name"],
            customer_name=row["Customer_Name"],
            hours=row["Hours"],
        )

    analyzer.stop()
