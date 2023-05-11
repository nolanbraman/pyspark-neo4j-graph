# Centus Data Cleanup, Query, and Property Graph Takehome

![Centus Exploratory Graph](https://i.imgur.com/m2EFQwy.png)

## Running the project

You will need to have hadoop and spark installed. You can find the links here.

What you will need:
[Java](https://kontext.tech/article/621/install-open-jdk-on-wsl)
[Hadoop](https://phoenixnap.com/kb/install-hadoop-ubuntu)
[Spark](https://spark.apache.org/downloads.html)

There are two current ways to run the project.

The first is to use poetry, [which has installation docs here](https://python-poetry.org/docs/) and then run  
`poetry install`  
`poetry shell`  
`python app/main.py`

The other is to use the requirements.txt sourced from the poetry file. This is  
`pip install requirements.txt`.
`python app/main.py`

This will likely clutter your dev environment, and peotry is the cleaner option, but I am also working on a dockerfile, time permitting to just run all of this in a self-contained way.

## Data cleanup

There are a few design decisions. First is a relatively minor choice of not dropping rows of data that were missing lat and lon in Demo Data- Task Management System - Projects.csv

The null fields are instead fed a placeholder value of 0.0, 0.0. There's an argument here to just leave them `Null` or `None`, and there is also an argument to use dropna() on the rows missing fields, but lat and lon seemed relatively minor in the scheme of things and could be filled in 'later'. Another way to have done this would be to do dropna(thresh=3), to drop any rows missing values for 3/4 columns. There's also a saved csv that completely drops the lat and lon, which decreases size taken by the db and some computational cycles. But, that's an argument for what this data is exactly supposed to do, and seemed acceptable to leave for the first iteration.

Next, I left column headers alone in terms of casing. It's likely better practice to have all columns the same casing, but for this situation, that seemed more of a detail to fix at the end, if ever.

## Database

database used for this takehome is Neo4j. If you would like to create your own instance, you can create a free sandbox at https://sandbox.neo4j.com/

### Two commands that will come in handy:

### Delete data:

```
match(c:Customer)-[r:OWNS]-() delete r;
match(c)-[r2:HAS_INVOICE]->() delete r2;
match(e)-[r3:WORKED_FOR]->(c) WHERE type(r3) = 'WORKED_FOR'  delete r3;
match(e)-[r4:WORKED_ON ]->(p) WHERE type(r4) = 'WORKED_ON'  delete r4;
match(c:Customer) delete c ;
match(p:Project) delete p ;
match(i:Invoice) delete i;
match(e:Employee) delete e;
```

### Show graph

`match (n) return n`

this will display all nodes and relationships, up to the limit set in settings. If you want to display more than 300 nodes, click the settings icon in the bottom left of the sandbox, and increase to whatever number you feel is reasonable.
