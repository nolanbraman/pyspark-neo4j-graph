from neo4j import GraphDatabase, basic_auth


class Neo4jDriver:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(
            uri, auth=basic_auth(user, password), encrypted=False
        )

    def close(self):
        self.driver.close()

    def wipe_data(self):
        with self.driver.session() as session:
            session.execute_write(self._wipe_data)

    def add_customer(self, customer_name: str, project_name: str, lat: int, lon: int):
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
        with self.driver.session() as session:
            session.execute_write(
                self._add_employees, employee_name, project_name, customer_name, hours
            )

    @staticmethod
    def _wipe_data(tx):
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
        tx.run(
            """
            MERGE (employee:Employee {name: $employee_name}) 
            MERGE (customer:Customer {name: $customer_name})
            MERGE (employee)-[:WORKED_FOR]->(customer)
            """,
            employee_name=employee_name,
            customer_name=customer_name,
        )

        tx.run(
            """
            MATCH (employee:Employee {name: $employee_name})
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
            MATCH (customer:Customer {name: $customer_name})
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
        tx.run(
            "CREATE (a:Project) "
            "SET a.project_name = $project_name "
            "RETURN a.project_name + ', from node ' + id(a)",
            project_name=project_name,
        )


if __name__ == "__main__":
    greeter = Neo4jDriver(
        "bolt://3.88.131.222:7687", "neo4j", "alcoholics-conditions-tops"
    )
    greeter.close()
