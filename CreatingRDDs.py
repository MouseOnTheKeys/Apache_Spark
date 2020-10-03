# Initializing a SQLContext
from pyspark.sql import SQLContext

sqlctx = SQLContext(sc)

# From within the pyspark shell, the following methods can be used to load
# the data from the employees table into an RDD
sqlContext.load(path=None, source=None, schema=None, **options)

# Creating an RDD from a JDBC Datasource Using the
# load() Function
employeesdf = sqlctx.load(
    source="jdbc",
    url="jdbc:mysql://localhost:3306/employees?user=<user>&password=<pwd>",
    dbtable="employees",
    partitionColumn="emp_no",
    numPartitions="2",
    lowerBound="10001",
    upperBound="499999",
)
employeesdf.rdd.getNumPartitions()
# should return 2 the following as we specified numPartitions=2

# Running SQL Queries Against Spark DataFrames
sqlctx.registerDataFrameAsTable(employeesdf, "employees")
df2 = sqlctx.sql("SELECT emp_no, first_name, last_name FROM employees LIMIT 2")
df2.show()

# Creating an RDD from a JDBC Datasource Using the read.jdbc() Function
employeesdf = sqlctx.read.jdbc(
    url="jdbc:mysql://localhost:3306/employees",
    table="employees",
    column="emp_no",
    numPartitions="2",
    lowerBound="10001",
    upperBound="499999",
    properties={"user": "<user>", "password": "<pwd>"},
)
employeesdf.rdd.getNumPartitions()

#! Creating RDDs from JSON files
sqlContext.jsonFile(path, schema=None)
sqlContext.read.json(path, schema=None)

# Creating and Working with an RDD Created from a JSON File
from pyspark.sql import SQLContext

sqlctx = SQLContext(sc)
people = sqlctx.jsonFile("/opt/spark/examples/src/main/resources/people.json")
people
# notice that the RDD created is a DataFrame which includes aschema
# DataFrame[age: bigint, name: string]
people.dtypes
# the dtypes method returns the column names and datatypes
# [('age', 'bigint'), ('name', 'string')]
people.show()

# as with all DataFrames you can create use them to run SQL
# queries
sqlctx.registerDataFrameAsTable(people, "people")
df2 = sqlctx.sql("SELECT name, age FROM people WHERE age > 20")
df2.show()

#! Creating an RDD Programatically
sc.parallelize(c, numSlices=None)

# Creating an RDD Using the parallelize() Method
parallelrdd = sc.parallelize([0, 1, 2, 3, 4, 5, 6, 7, 8])
parallelrdd
# notice the type of RDD created
# ParallelCollectionRDD[0] at parallelize at PythonRDD.scala:423
parallelrdd.min()
# will return 0 as this was the min value in our list
parallelrdd.max()
# will return 8 as this was the max value in our list
parallelrdd.collect()
# will return the parallel collection as a list
# [0, 1, 2, 3, 4, 5, 6, 7, 8]

#! Creating an RDD Using the range() Method
# create an RDD using the range() function
# with 1000 integers starting at 0 in increments of 1
# across 2 partitions
rangerdd = sc.range(0, 1000, 1, 2)
rangerdd
# note the PythonRDD type, as range is a native Python function
# PythonRDD[1] at RDD at PythonRDD.scala:43
rangerdd.getNumPartitions()
# should return 2 as we requested numSlices=2
rangerdd.min()
# should return 0 as this was out start argument
rangerdd.max()
# should return 999 as this is 1000 increments of 1 starting from 0
rangerdd.take(5)
# should return [0, 1, 2, 3, 4]
