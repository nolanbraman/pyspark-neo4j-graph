from neo4j import GraphDatabase, basic_auth


class Neo4jDriver:
    def __init__(self, uri, user, password):
        """
        Initialize the Neo4jDriver object.

        Args:
            uri (str): URI of the Neo4j database.
            user (str): Username for authentication.
            password (str): Password for authentication.
        """
        self.driver = GraphDatabase.driver(
            uri, auth=basic_auth(user, password), encrypted=False
        )

    def close(self):
        """Close the Neo4j driver connection."""
        self.driver.close()

    def wipe_data(self):
        """Wipe the data in the Neo4j database."""
        with self.driver.session() as session:
            session.execute_write(self._wipe_data)

    def add_customer(self, customer_name: str, project_name: str, lat: int, lon: int):
        """
        Add a customer node and its relationships to the Neo4j database.

        Args:
            customer_name (str): Name of the customer.
            project_name (str): Name of the project.
            lat (int): Latitude.
            lon (int): Longitude.
        """
        with self.driver.session() as session:
            session.execute_write(
                self._create_customers_and_relationships,
                customer_name,
                project_name,
                lat,
                lon,
            )

    def add_business_ops(
        self,
        customer_name: str,
        invoice_number: int,
        invoice_amount: float,
        invoice_date: str,
    ):
        """
        Add business operations data to the Neo4j database.

        Args:
            customer_name (str): Name of the customer.
            invoice_number (int): Invoice number.
            invoice_amount (float): Invoice amount.
            invoice_date (str): Invoice date.
        """
        with self.driver.session() as session:
            session.execute_write(
                self._create_business_ops,
                customer_name,
                invoice_number,
                invoice_amount,
                invoice_date,
            )

    def add_employees(
        self, employee_name: str, project_name: str, customer_name: str, hours: float
    ):
        """
        Add employee data and relationships to the Neo4j database.

        Args:
            employee_name (str): Name of the employee.
            project_name (str): Name of the project.
            customer_name (str): Name of the customer.
            hours (float): Number of hours worked.
        """
        with self.driver.session() as session:
            session.execute_write(
                self._add_employees, employee_name, project_name, customer_name, hours
            )

    @staticmethod
    def _wipe_data(tx):
        """
        Wipe the data in the Neo4j database.

        Args:
            tx: Neo4j transaction.
        """
        tx.run("match(c:Customer)-[r:OWNS]-() delete r;")
        tx.run("match(c)-[r2:HAS_INVOICE]->() delete r2;")
        tx.run(
            "match(e)-[r3:WORKED_FOR]->(c) WHERE type(r3) = 'WORKED_FOR'  delete r3;"
        )
        tx.run("match(e)-[r4:WORKED_ON ]->(p) WHERE type(r4) = 'WORKED_ON'  delete r4;")
        tx.run("match(c:Customer) delete c ;")
        tx.run("match(p:Project) delete p ;")
        tx.run("match(i:Invoice) delete i;")
        tx.run("match(e:Employee) delete e;")

    @staticmethod
    def _create_customers_and_relationships(
        tx, customer_name: str, project_name: str, lat: int, lon: int
    ) -> None:
        """
        Create customer nodes and relationships in the Neo4j database.

        Args:
            tx: Neo4j transaction.
            customer_name (str): Name of the customer.
            project_name (str): Name of the project.
            lat (int): Latitude.
            lon (int): Longitude.
        """
        tx.run(
            "MERGE (c:Customer {customer_name: $customer_name}) "
            "ON CREATE SET c.lat = $lat, c.lon = $lon",
            customer_name=customer_name,
            lat=lat,
            lon=lon,
        )

        tx.run(
            "MERGE (p:Project {project_name: $project_name})",
            project_name=project_name,
        )

        tx.run(
            "MATCH (c:Customer), (p:Project) "
            "WHERE c.customer_name = $customer_name AND p.project_name = $project_name "
            "MERGE (c)-[r:OWNS]->(p)",
            project_name=project_name,
            customer_name=customer_name,
        )

    @staticmethod
    def _add_employees(
        tx, employee_name: str, project_name: str, customer_name: str, hours: float
    ):
        """
        Add employee nodes and relationships in the Neo4j database.

        Args:
            tx: Neo4j transaction.
            employee_name (str): Name of the employee.
            project_name (str): Name of the project.
            customer_name (str): Name of the customer.
            hours (float): Number of hours worked.
        """
        tx.run(
            """
            MERGE (employee:Employee {employee_name: $employee_name}) 
            MERGE (customer:Customer {customer_name: $customer_name})
            MERGE (employee)-[:WORKED_FOR]->(customer)
            """,
            employee_name=employee_name,
            customer_name=customer_name,
        )

        tx.run(
            """
            MATCH (employee:Employee {employee_name: $employee_name})
            MATCH (project:Project {project_name: $project_name})
            MERGE (employee)-[r:WORKED_ON ]->(project)
            ON CREATE SET r.hours = $hours
            ON MATCH SET r.hours = r.hours + $hours
            """,
            employee_name=employee_name,
            project_name=project_name,
            hours=hours,
        )

        tx.run(
            """
            MATCH (customer:Customer {customer_name: $customer_name})
            MATCH (company:Company)<-[:OWNS]-(customer)
            MERGE (customer)-[:BELONGS_TO]->(company)
            """,
            customer_name=customer_name,
        )

    @staticmethod
    def _create_business_ops(
        tx,
        customer_name: str,
        invoice_number: int,
        invoice_amount: float,
        invoice_date: str,
    ):
        """
        Create business operations data in the Neo4j database.

        Args:
            tx: Neo4j transaction.
            customer_name (str): Name of the customer.
            invoice_number (int): Invoice number.
            invoice_amount (float): Invoice amount.
            invoice_date (str): Invoice date.
        """
        tx.run(
            "MERGE (c:Customer {customer_name: $customer_name}) "
            "MERGE (i:Invoice {invoice_number: $invoice_number, invoice_date: $invoice_date, invoice_amount: $invoice_amount}) "
            "MERGE (c)-[r:HAS_INVOICE]->(i)",
            customer_name=customer_name,
            invoice_number=invoice_number,
            invoice_amount=invoice_amount,
            invoice_date=invoice_date,
        )
        tx.run(
            "MERGE (c:Customer {customer_name: $customer_name}) "
            "MERGE (i:Invoice {invoice_number: $invoice_number, invoice_date: $invoice_date, invoice_amount: $invoice_amount}) "
            "MERGE (c)-[r:HAS_INVOICE]->(i)",
            customer_name=customer_name,
            invoice_number=invoice_number,
            invoice_amount=invoice_amount,
            invoice_date=invoice_date,
        )

    @staticmethod
    def _create_project(tx, project_name: str) -> None:
        """
        Create a project node in the Neo4j database.

        Args:
            tx: Neo4j transaction.
            project_name (str): Name of the project.
        """
        tx.run(
            "CREATE (a:Project) "
            "SET a.project_name = $project_name "
            "RETURN a.project_name + ', from node ' + id(a)",
            project_name=project_name,
        )


if __name__ == "__main__":
    # Example usage of Neo4jDriver class
    greeter = Neo4jDriver(
        "url", "neo4j", "pwd"
    )
    greeter.close()
