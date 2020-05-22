# PYSparkexample
Pyspark self learning examaple..
Important classes of Spark SQL and DataFrames:

pyspark.sql.SparkSession Main entry point for DataFrame and SQL functionality.

pyspark.sql.DataFrame A distributed collection of data grouped into named columns.

pyspark.sql.Column A column expression in a DataFrame.

pyspark.sql.Row A row of data in a DataFrame.

pyspark.sql.GroupedData Aggregation methods, returned by DataFrame.groupBy().

pyspark.sql.DataFrameNaFunctions Methods for handling missing data (null values).

pyspark.sql.DataFrameStatFunctions Methods for statistics functionality.

pyspark.sql.functions List of built-in functions available for DataFrame.

pyspark.sql.types List of data types available.

pyspark.sql.Window For working with window functions.

class pyspark.sql.SparkSession(sparkContext, jsparkSession=None)[source]
The entry point to programming Spark with the Dataset and DataFrame API.

A SparkSession can be used create DataFrame, register DataFrame as tables, execute SQL over tables, cache tables, and read parquet files. To create a SparkSession, use the following builder pattern:

spark = SparkSession.builder \
    .master("local") \
    .appName("Word Count") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
builder
A class attribute having a Builder to construct SparkSession instances.

class Builder[source]
Builder for SparkSession.

appName(name)[source]
Sets a name for the application, which will be shown in the Spark web UI.

If no application name is set, a randomly generated name will be used.

Parameters
name – an application name

New in version 2.0.

config(key=None, value=None, conf=None)[source]
Sets a config option. Options set using this method are automatically propagated to both SparkConf and SparkSession’s own configuration.

For an existing SparkConf, use conf parameter.

from pyspark.conf import SparkConf
SparkSession.builder.config(conf=SparkConf())
<pyspark.sql.session...
For a (key, value) pair, you can omit parameter names.

SparkSession.builder.config("spark.some.config.option", "some-value")
<pyspark.sql.session...
Parameters
key – a key name string for configuration property

value – a value for configuration property

conf – an instance of SparkConf

New in version 2.0.

enableHiveSupport()[source]
Enables Hive support, including connectivity to a persistent Hive metastore, support for Hive SerDes, and Hive user-defined functions.

New in version 2.0.

getOrCreate()[source]
Gets an existing SparkSession or, if there is no existing one, creates a new one based on the options set in this builder.

This method first checks whether there is a valid global default SparkSession, and if yes, return that one. If no valid global default SparkSession exists, the method creates a new SparkSession and assigns the newly created SparkSession as the global default.

s1 = SparkSession.builder.config("k1", "v1").getOrCreate()
s1.conf.get("k1") == s1.sparkContext.getConf().get("k1") == "v1"
True
In case an existing SparkSession is returned, the config options specified in this builder will be applied to the existing SparkSession.

s2 = SparkSession.builder.config("k2", "v2").getOrCreate()
s1.conf.get("k1") == s2.conf.get("k1")
True
s1.conf.get("k2") == s2.conf.get("k2")
True
New in version 2.0.

master(master)[source]
Sets the Spark master URL to connect to, such as “local” to run locally, “local[4]” to run locally with 4 cores, or “spark://master:7077” to run on a Spark standalone cluster.

Parameters
master – a url for spark master

New in version 2.0.

property catalog
Interface through which the user may create, drop, alter or query underlying databases, tables, functions, etc.

Returns
Catalog

New in version 2.0.

property conf
Runtime configuration interface for Spark.

This is the interface through which the user can get and set all Spark and Hadoop configurations that are relevant to Spark SQL. When getting the value of a config, this defaults to the value set in the underlying SparkContext, if any.

New in version 2.0.

createDataFrame(data, schema=None, samplingRatio=None, verifySchema=True)[source]
Creates a DataFrame from an RDD, a list or a pandas.DataFrame.

When schema is a list of column names, the type of each column will be inferred from data.

When schema is None, it will try to infer the schema (column names and types) from data, which should be an RDD of either Row, namedtuple, or dict.

When schema is pyspark.sql.types.DataType or a datatype string, it must match the real data, or an exception will be thrown at runtime. If the given schema is not pyspark.sql.types.StructType, it will be wrapped into a pyspark.sql.types.StructType as its only field, and the field name will be “value”. Each record will also be wrapped into a tuple, which can be converted to row later.

If schema inference is needed, samplingRatio is used to determined the ratio of rows used for schema inference. The first row will be used if samplingRatio is None.

Parameters
data – an RDD of any kind of SQL data representation (e.g. row, tuple, int, boolean, etc.), list, or pandas.DataFrame.

schema – a pyspark.sql.types.DataType or a datatype string or a list of column names, default is None. The data type string format equals to pyspark.sql.types.DataType.simpleString, except that top level struct type can omit the struct<> and atomic types use typeName() as their format, e.g. use byte instead of tinyint for pyspark.sql.types.ByteType. We can also use int as a short name for IntegerType.

samplingRatio – the sample ratio of rows used for inferring

verifySchema – verify data types of every row against schema.

Returns
DataFrame

Changed in version 2.1: Added verifySchema.

Note Usage with spark.sql.execution.arrow.enabled=True is experimental.
l = [('Alice', 1)]
spark.createDataFrame(l).collect()
[Row(_1='Alice', _2=1)]
spark.createDataFrame(l, ['name', 'age']).collect()
[Row(name='Alice', age=1)]
d = [{'name': 'Alice', 'age': 1}]
spark.createDataFrame(d).collect()
[Row(age=1, name='Alice')]
rdd = sc.parallelize(l)
spark.createDataFrame(rdd).collect()
[Row(_1='Alice', _2=1)]
df = spark.createDataFrame(rdd, ['name', 'age'])
df.collect()
[Row(name='Alice', age=1)]
from pyspark.sql import Row
Person = Row('name', 'age')
person = rdd.map(lambda r: Person(*r))
df2 = spark.createDataFrame(person)
df2.collect()
[Row(name='Alice', age=1)]
from pyspark.sql.types import *
schema = StructType([
   StructField("name", StringType(), True),
   StructField("age", IntegerType(), True)])
df3 = spark.createDataFrame(rdd, schema)
df3.collect()
[Row(name='Alice', age=1)]
spark.createDataFrame(df.toPandas()).collect()  
[Row(name='Alice', age=1)]
spark.createDataFrame(pandas.DataFrame([[1, 2]])).collect()  
[Row(0=1, 1=2)]
spark.createDataFrame(rdd, "a: string, b: int").collect()
[Row(a='Alice', b=1)]
rdd = rdd.map(lambda row: row[1])
spark.createDataFrame(rdd, "int").collect()
[Row(value=1)]
spark.createDataFrame(rdd, "boolean").collect() 
Traceback (most recent call last):
    ...
Py4JJavaError: ...
New in version 2.0.

newSession()[source]
Returns a new SparkSession as new session, that has separate SQLConf, registered temporary views and UDFs, but shared SparkContext and table cache.

New in version 2.0.

range(start, end=None, step=1, numPartitions=None)[source]
Create a DataFrame with single pyspark.sql.types.LongType column named id, containing elements in a range from start to end (exclusive) with step value step.

Parameters
start – the start value

end – the end value (exclusive)

step – the incremental step (default: 1)

numPartitions – the number of partitions of the DataFrame

Returns
DataFrame

spark.range(1, 7, 2).collect()
[Row(id=1), Row(id=3), Row(id=5)]
If only one argument is specified, it will be used as the end value.

spark.range(3).collect()
[Row(id=0), Row(id=1), Row(id=2)]
New in version 2.0.

property read
Returns a DataFrameReader that can be used to read data in as a DataFrame.

Returns
DataFrameReader

New in version 2.0.

property readStream
Returns a DataStreamReader that can be used to read data streams as a streaming DataFrame.

Note Evolving.
Returns
DataStreamReader

New in version 2.0.

property sparkContext
Returns the underlying SparkContext.

New in version 2.0.

sql(sqlQuery)[source]
Returns a DataFrame representing the result of the given query.

Returns
DataFrame

df.createOrReplaceTempView("table1")
df2 = spark.sql("SELECT field1 AS f1, field2 as f2 from table1")
df2.collect()
[Row(f1=1, f2='row1'), Row(f1=2, f2='row2'), Row(f1=3, f2='row3')]
New in version 2.0.

stop()[source]
Stop the underlying SparkContext.

New in version 2.0.

property streams
Returns a StreamingQueryManager that allows managing all the StreamingQuery instances active on this context.

Note Evolving.
Returns
StreamingQueryManager

New in version 2.0.

table(tableName)[source]
Returns the specified table as a DataFrame.

Returns
DataFrame

df.createOrReplaceTempView("table1")
df2 = spark.table("table1")
sorted(df.collect()) == sorted(df2.collect())
True
New in version 2.0.

property udf
Returns a UDFRegistration for UDF registration.

Returns
UDFRegistration

New in version 2.0.

property version
The version of Spark on which this application is running.

New in version 2.0.

class pyspark.sql.SQLContext(sparkContext, sparkSession=None, jsqlContext=None)[source]
The entry point for working with structured data (rows and columns) in Spark, in Spark 1.x.

As of Spark 2.0, this is replaced by SparkSession. However, we are keeping the class here for backward compatibility.

A SQLContext can be used create DataFrame, register DataFrame as tables, execute SQL over tables, cache tables, and read parquet files.

Parameters
sparkContext – The SparkContext backing this SQLContext.

sparkSession – The SparkSession around which this SQLContext wraps.

jsqlContext – An optional JVM Scala SQLContext. If set, we do not instantiate a new SQLContext in the JVM, instead we make all calls to this object.

cacheTable(tableName)[source]
Caches the specified table in-memory.

New in version 1.0.

clearCache()[source]
Removes all cached tables from the in-memory cache.

New in version 1.3.

createDataFrame(data, schema=None, samplingRatio=None, verifySchema=True)[source]
Creates a DataFrame from an RDD, a list or a pandas.DataFrame.

When schema is a list of column names, the type of each column will be inferred from data.

When schema is None, it will try to infer the schema (column names and types) from data, which should be an RDD of Row, or namedtuple, or dict.

When schema is pyspark.sql.types.DataType or a datatype string it must match the real data, or an exception will be thrown at runtime. If the given schema is not pyspark.sql.types.StructType, it will be wrapped into a pyspark.sql.types.StructType as its only field, and the field name will be “value”, each record will also be wrapped into a tuple, which can be converted to row later.

If schema inference is needed, samplingRatio is used to determined the ratio of rows used for schema inference. The first row will be used if samplingRatio is None.

Parameters
data – an RDD of any kind of SQL data representation(e.g. Row, tuple, int, boolean, etc.), or list, or pandas.DataFrame.

schema – a pyspark.sql.types.DataType or a datatype string or a list of column names, default is None. The data type string format equals to pyspark.sql.types.DataType.simpleString, except that top level struct type can omit the struct<> and atomic types use typeName() as their format, e.g. use byte instead of tinyint for pyspark.sql.types.ByteType. We can also use int as a short name for pyspark.sql.types.IntegerType.

samplingRatio – the sample ratio of rows used for inferring

verifySchema – verify data types of every row against schema.

Returns
DataFrame

Changed in version 2.0: The schema parameter can be a pyspark.sql.types.DataType or a datatype string after 2.0. If it’s not a pyspark.sql.types.StructType, it will be wrapped into a pyspark.sql.types.StructType and each record will also be wrapped into a tuple.

Changed in version 2.1: Added verifySchema.

l = [('Alice', 1)]
sqlContext.createDataFrame(l).collect()
[Row(_1='Alice', _2=1)]
sqlContext.createDataFrame(l, ['name', 'age']).collect()
[Row(name='Alice', age=1)]
d = [{'name': 'Alice', 'age': 1}]
sqlContext.createDataFrame(d).collect()
[Row(age=1, name='Alice')]
rdd = sc.parallelize(l)
sqlContext.createDataFrame(rdd).collect()
[Row(_1='Alice', _2=1)]
df = sqlContext.createDataFrame(rdd, ['name', 'age'])
df.collect()
[Row(name='Alice', age=1)]
from pyspark.sql import Row
Person = Row('name', 'age')
person = rdd.map(lambda r: Person(*r))
df2 = sqlContext.createDataFrame(person)
df2.collect()
[Row(name='Alice', age=1)]
from pyspark.sql.types import *
schema = StructType([
   StructField("name", StringType(), True),
   StructField("age", IntegerType(), True)])
df3 = sqlContext.createDataFrame(rdd, schema)
df3.collect()
[Row(name='Alice', age=1)]
sqlContext.createDataFrame(df.toPandas()).collect()  
[Row(name='Alice', age=1)]
sqlContext.createDataFrame(pandas.DataFrame([[1, 2]])).collect()  
[Row(0=1, 1=2)]
sqlContext.createDataFrame(rdd, "a: string, b: int").collect()
[Row(a='Alice', b=1)]
rdd = rdd.map(lambda row: row[1])
sqlContext.createDataFrame(rdd, "int").collect()
[Row(value=1)]
sqlContext.createDataFrame(rdd, "boolean").collect() 
Traceback (most recent call last):
    ...
Py4JJavaError: ...
New in version 1.3.

createExternalTable(tableName, path=None, source=None, schema=None, **options)[source]
Creates an external table based on the dataset in a data source.

It returns the DataFrame associated with the external table.

The data source is specified by the source and a set of options. If source is not specified, the default data source configured by spark.sql.sources.default will be used.

Optionally, a schema can be provided as the schema of the returned DataFrame and created external table.

Returns
DataFrame

New in version 1.3.

dropTempTable(tableName)[source]
Remove the temporary table from catalog.

sqlContext.registerDataFrameAsTable(df, "table1")
sqlContext.dropTempTable("table1")
New in version 1.6.

getConf(key, defaultValue=<no value>)[source]
Returns the value of Spark SQL configuration property for the given key.

If the key is not set and defaultValue is set, return defaultValue. If the key is not set and defaultValue is not set, return the system default value.

sqlContext.getConf("spark.sql.shuffle.partitions")
'200'
sqlContext.getConf("spark.sql.shuffle.partitions", u"10")
'10'
sqlContext.setConf("spark.sql.shuffle.partitions", u"50")
sqlContext.getConf("spark.sql.shuffle.partitions", u"10")
'50'
New in version 1.3.

classmethod getOrCreate(sc)[source]
Get the existing SQLContext or create a new one with given SparkContext.

Parameters
sc – SparkContext

New in version 1.6.

newSession()[source]
Returns a new SQLContext as new session, that has separate SQLConf, registered temporary views and UDFs, but shared SparkContext and table cache.

New in version 1.6.

range(start, end=None, step=1, numPartitions=None)[source]
Create a DataFrame with single pyspark.sql.types.LongType column named id, containing elements in a range from start to end (exclusive) with step value step.

Parameters
start – the start value

end – the end value (exclusive)

step – the incremental step (default: 1)

numPartitions – the number of partitions of the DataFrame

Returns
DataFrame

sqlContext.range(1, 7, 2).collect()
[Row(id=1), Row(id=3), Row(id=5)]
If only one argument is specified, it will be used as the end value.

sqlContext.range(3).collect()
[Row(id=0), Row(id=1), Row(id=2)]
New in version 1.4.

property read
Returns a DataFrameReader that can be used to read data in as a DataFrame.

Returns
DataFrameReader

New in version 1.4.

property readStream
Returns a DataStreamReader that can be used to read data streams as a streaming DataFrame.

Note Evolving.
Returns
DataStreamReader

text_sdf = sqlContext.readStream.text(tempfile.mkdtemp())
text_sdf.isStreaming
True
New in version 2.0.

registerDataFrameAsTable(df, tableName)[source]
Registers the given DataFrame as a temporary table in the catalog.

Temporary tables exist only during the lifetime of this instance of SQLContext.

sqlContext.registerDataFrameAsTable(df, "table1")
New in version 1.3.

registerFunction(name, f, returnType=None)[source]
An alias for spark.udf.register(). See pyspark.sql.UDFRegistration.register().

Note Deprecated in 2.3.0. Use spark.udf.register() instead.
New in version 1.2.

registerJavaFunction(name, javaClassName, returnType=None)[source]
An alias for spark.udf.registerJavaFunction(). See pyspark.sql.UDFRegistration.registerJavaFunction().

Note Deprecated in 2.3.0. Use spark.udf.registerJavaFunction() instead.
New in version 2.1.

setConf(key, value)[source]
Sets the given Spark SQL configuration property.

New in version 1.3.

sql(sqlQuery)[source]
Returns a DataFrame representing the result of the given query.

Returns
DataFrame

sqlContext.registerDataFrameAsTable(df, "table1")
df2 = sqlContext.sql("SELECT field1 AS f1, field2 as f2 from table1")
df2.collect()
[Row(f1=1, f2='row1'), Row(f1=2, f2='row2'), Row(f1=3, f2='row3')]
New in version 1.0.

property streams
Returns a StreamingQueryManager that allows managing all the StreamingQuery StreamingQueries active on this context.

Note Evolving.
New in version 2.0.

table(tableName)[source]
Returns the specified table or view as a DataFrame.

Returns
DataFrame

sqlContext.registerDataFrameAsTable(df, "table1")
df2 = sqlContext.table("table1")
sorted(df.collect()) == sorted(df2.collect())
True
New in version 1.0.

tableNames(dbName=None)[source]
Returns a list of names of tables in the database dbName.

Parameters
dbName – string, name of the database to use. Default to the current database.

Returns
list of table names, in string

sqlContext.registerDataFrameAsTable(df, "table1")
"table1" in sqlContext.tableNames()
True
"table1" in sqlContext.tableNames("default")
True
New in version 1.3.

tables(dbName=None)[source]
Returns a DataFrame containing names of tables in the given database.

If dbName is not specified, the current database will be used.

The returned DataFrame has two columns: tableName and isTemporary (a column with BooleanType indicating if a table is a temporary one or not).

Parameters
dbName – string, name of the database to use.

Returns
DataFrame

sqlContext.registerDataFrameAsTable(df, "table1")
df2 = sqlContext.tables()
df2.filter("tableName = 'table1'").first()
Row(database='', tableName='table1', isTemporary=True)
New in version 1.3.

property udf
Returns a UDFRegistration for UDF registration.

Returns
UDFRegistration

New in version 1.3.1.

uncacheTable(tableName)[source]
Removes the specified table from the in-memory cache.

New in version 1.0.

class pyspark.sql.HiveContext(sparkContext, jhiveContext=None)[source]
A variant of Spark SQL that integrates with data stored in Hive.

Configuration for Hive is read from hive-site.xml on the classpath. It supports running both SQL and HiveQL commands.

Parameters
sparkContext – The SparkContext to wrap.

jhiveContext – An optional JVM Scala HiveContext. If set, we do not instantiate a new HiveContext in the JVM, instead we make all calls to this object.

Note Deprecated in 2.0.0. Use SparkSession.builder.enableHiveSupport().getOrCreate().
refreshTable(tableName)[source]
Invalidate and refresh all the cached the metadata of the given table. For performance reasons, Spark SQL or the external data source library it uses might cache certain metadata about a table, such as the location of blocks. When those change outside of Spark SQL, users should call this function to invalidate the cache.

class pyspark.sql.UDFRegistration(sparkSession)[source]
Wrapper for user-defined function registration. This instance can be accessed by spark.udf or sqlContext.udf.

New in version 1.3.1.

register(name, f, returnType=None)[source]
Register a Python function (including lambda function) or a user-defined function as a SQL function.

Parameters
name – name of the user-defined function in SQL statements.

f – a Python function, or a user-defined function. The user-defined function can be either row-at-a-time or vectorized. See pyspark.sql.functions.udf() and pyspark.sql.functions.pandas_udf().

returnType – the return type of the registered user-defined function. The value can be either a pyspark.sql.types.DataType object or a DDL-formatted type string.

Returns
a user-defined function.

To register a nondeterministic Python function, users need to first build a nondeterministic user-defined function for the Python function and then register it as a SQL function.

returnType can be optionally specified when f is a Python function but not when f is a user-defined function. Please see below.

When f is a Python function:

returnType defaults to string type and can be optionally specified. The produced object must match the specified type. In this case, this API works as if register(name, f, returnType=StringType()).

strlen = spark.udf.register("stringLengthString", lambda x: len(x))
spark.sql("SELECT stringLengthString('test')").collect()
[Row(stringLengthString(test)='4')]
spark.sql("SELECT 'foo' AS text").select(strlen("text")).collect()
[Row(stringLengthString(text)='3')]
from pyspark.sql.types import IntegerType
_ = spark.udf.register("stringLengthInt", lambda x: len(x), IntegerType())
spark.sql("SELECT stringLengthInt('test')").collect()
[Row(stringLengthInt(test)=4)]
from pyspark.sql.types import IntegerType
_ = spark.udf.register("stringLengthInt", lambda x: len(x), IntegerType())
spark.sql("SELECT stringLengthInt('test')").collect()
[Row(stringLengthInt(test)=4)]
When f is a user-defined function:

Spark uses the return type of the given user-defined function as the return type of the registered user-defined function. returnType should not be specified. In this case, this API works as if register(name, f).

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf
slen = udf(lambda s: len(s), IntegerType())
_ = spark.udf.register("slen", slen)
spark.sql("SELECT slen('test')").collect()
[Row(slen(test)=4)]
import random
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
random_udf = udf(lambda: random.randint(0, 100), IntegerType()).asNondeterministic()
new_random_udf = spark.udf.register("random_udf", random_udf)
spark.sql("SELECT random_udf()").collect()  
[Row(random_udf()=82)]
from pyspark.sql.functions import pandas_udf, PandasUDFType
:pandas_udf("integer", PandasUDFType.SCALAR)  
def add_one(x):
    return x + 1

_ = spark.udf.register("add_one", add_one)  
spark.sql("SELECT add_one(id) FROM range(3)").collect()  
[Row(add_one(id)=1), Row(add_one(id)=2), Row(add_one(id)=3)]
:pandas_udf("integer", PandasUDFType.GROUPED_AGG)  
def sum_udf(v):
    return v.sum()

_ = spark.udf.register("sum_udf", sum_udf)  
q = "SELECT sum_udf(v1) FROM VALUES (3, 0), (2, 0), (1, 1) tbl(v1, v2) GROUP BY v2"
spark.sql(q).collect()  
[Row(sum_udf(v1)=1), Row(sum_udf(v1)=5)]
Note Registration for a user-defined function (case 2.) was added from Spark 2.3.0.
New in version 1.3.1.

registerJavaFunction(name, javaClassName, returnType=None)[source]
Register a Java user-defined function as a SQL function.

In addition to a name and the function itself, the return type can be optionally specified. When the return type is not specified we would infer it via reflection.

Parameters
name – name of the user-defined function

javaClassName – fully qualified name of java class

returnType – the return type of the registered Java function. The value can be either a pyspark.sql.types.DataType object or a DDL-formatted type string.

from pyspark.sql.types import IntegerType
spark.udf.registerJavaFunction(
    "javaStringLength", "test.org.apache.spark.sql.JavaStringLength", IntegerType())
spark.sql("SELECT javaStringLength('test')").collect()
[Row(UDF:javaStringLength(test)=4)]
spark.udf.registerJavaFunction(
    "javaStringLength2", "test.org.apache.spark.sql.JavaStringLength")
spark.sql("SELECT javaStringLength2('test')").collect()
[Row(UDF:javaStringLength2(test)=4)]
spark.udf.registerJavaFunction(
    "javaStringLength3", "test.org.apache.spark.sql.JavaStringLength", "integer")
spark.sql("SELECT javaStringLength3('test')").collect()
[Row(UDF:javaStringLength3(test)=4)]
New in version 2.3.

registerJavaUDAF(name, javaClassName)[source]
Register a Java user-defined aggregate function as a SQL function.

Parameters
name – name of the user-defined aggregate function

javaClassName – fully qualified name of java class

spark.udf.registerJavaUDAF("javaUDAF", "test.org.apache.spark.sql.MyDoubleAvg")
df = spark.createDataFrame([(1, "a"),(2, "b"), (3, "a")],["id", "name"])
df.createOrReplaceTempView("df")
spark.sql("SELECT name, javaUDAF(id) as avg from df group by name").collect()
[Row(name='b', avg=102.0), Row(name='a', avg=102.0)]
New in version 2.3.

class pyspark.sql.DataFrame(jdf, sql_ctx)[source]
A distributed collection of data grouped into named columns.

A DataFrame is equivalent to a relational table in Spark SQL, and can be created using various functions in SparkSession:

people = spark.read.parquet("...")
Once created, it can be manipulated using the various domain-specific-language (DSL) functions defined in: DataFrame, Column.

To select a column from the DataFrame, use the apply method:

ageCol = people.age
A more concrete example:

# To create DataFrame using SparkSession
people = spark.read.parquet("...")
department = spark.read.parquet("...")

people.filter(people.age > 30).join(department, people.deptId == department.id) \
  .groupBy(department.name, "gender").agg({"salary": "avg", "age": "max"})
New in version 1.3.

agg(*exprs)[source]
Aggregate on the entire DataFrame without groups (shorthand for df.groupBy.agg()).

df.agg({"age": "max"}).collect()
[Row(max(age)=5)]
from pyspark.sql import functions as F
df.agg(F.min(df.age)).collect()
[Row(min(age)=2)]
New in version 1.3.

alias(alias)[source]
Returns a new DataFrame with an alias set.

Parameters
alias – string, an alias name to be set for the DataFrame.

from pyspark.sql.functions import *
df_as1 = df.alias("df_as1")
df_as2 = df.alias("df_as2")
joined_df = df_as1.join(df_as2, col("df_as1.name") == col("df_as2.name"), 'inner')
joined_df.select("df_as1.name", "df_as2.name", "df_as2.age").collect()
[Row(name='Bob', name='Bob', age=5), Row(name='Alice', name='Alice', age=2)]
New in version 1.3.

approxQuantile(col, probabilities, relativeError)[source]
Calculates the approximate quantiles of numerical columns of a DataFrame.

The result of this algorithm has the following deterministic bound: If the DataFrame has N elements and if we request the quantile at probability p up to error err, then the algorithm will return a sample x from the DataFrame so that the exact rank of x is close to (p * N). More precisely,

floor((p - err) * N) <= rank(x) <= ceil((p + err) * N).

This method implements a variation of the Greenwald-Khanna algorithm (with some speed optimizations). The algorithm was first present in [[http://dx.doi.org/10.1145/375663.375670 Space-efficient Online Computation of Quantile Summaries]] by Greenwald and Khanna.

Note that null values will be ignored in numerical columns before calculation. For columns only containing null values, an empty list is returned.

Parameters
col – str, list. Can be a single column name, or a list of names for multiple columns.

probabilities – a list of quantile probabilities Each number must belong to [0, 1]. For example 0 is the minimum, 0.5 is the median, 1 is the maximum.

relativeError – The relative target precision to achieve (>= 0). If set to zero, the exact quantiles are computed, which could be very expensive. Note that values greater than 1 are accepted but give the same result as 1.

Returns
the approximate quantiles at the given probabilities. If the input col is a string, the output is a list of floats. If the input col is a list or tuple of strings, the output is also a list, but each element in it is a list of floats, i.e., the output is a list of list of floats.

Changed in version 2.2: Added support for multiple columns.

New in version 2.0.

cache()[source]
Persists the DataFrame with the default storage level (MEMORY_AND_DISK).

Note The default storage level has changed to MEMORY_AND_DISK to match Scala in 2.0.
New in version 1.3.

checkpoint(eager=True)[source]
Returns a checkpointed version of this Dataset. Checkpointing can be used to truncate the logical plan of this DataFrame, which is especially useful in iterative algorithms where the plan may grow exponentially. It will be saved to files inside the checkpoint directory set with SparkContext.setCheckpointDir().

Parameters
eager – Whether to checkpoint this DataFrame immediately

Note Experimental
New in version 2.1.

coalesce(numPartitions)[source]
Returns a new DataFrame that has exactly numPartitions partitions.

Parameters
numPartitions – int, to specify the target number of partitions

Similar to coalesce defined on an RDD, this operation results in a narrow dependency, e.g. if you go from 1000 partitions to 100 partitions, there will not be a shuffle, instead each of the 100 new partitions will claim 10 of the current partitions. If a larger number of partitions is requested, it will stay at the current number of partitions.

However, if you’re doing a drastic coalesce, e.g. to numPartitions = 1, this may result in your computation taking place on fewer nodes than you like (e.g. one node in the case of numPartitions = 1). To avoid this, you can call repartition(). This will add a shuffle step, but means the current upstream partitions will be executed in parallel (per whatever the current partitioning is).

df.coalesce(1).rdd.getNumPartitions()
1
New in version 1.4.

colRegex(colName)[source]
Selects column based on the column name specified as a regex and returns it as Column.

Parameters
colName – string, column name specified as a regex.

df = spark.createDataFrame([("a", 1), ("b", 2), ("c",  3)], ["Col1", "Col2"])
df.select(df.colRegex("`(Col1)?+.+`")).show()
+----+
|Col2|
+----+
|   1|
|   2|
|   3|
+----+
New in version 2.3.

collect()[source]
Returns all the records as a list of Row.

df.collect()
[Row(age=2, name='Alice'), Row(age=5, name='Bob')]
New in version 1.3.
