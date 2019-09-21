// spark-shell -–driver-class-path /path-to-mysql-jar/mysql-connector-java-5.1.34-bin.jar

package com.eztier.examples

import org.apache.spark.{SparkConf,	SparkContext}
import org.apache.spark.sql.SQLContext
import java.util.Properties
import java.sql.Timestamp

case class ParticipantHist
(
id: Option[String],
from_store: Option[String],
_uid: Option[String],
start_time: Option[Timestamp],
mrn: Option[String],
firstName: Option[String],
lastName: Option[String],
dob: Option[String],
processState: Option[String],
streetAddressLine: Option[String],
city: Option[String],
state: Option[String],
postalCode: Option[String],
context: Option[String],
processed: Option[Int],
dateProcessed: Option[Timestamp],
response: Option[String]
)

object SimpleJdbcApp {
  def main(args: Array[String]) {
  
    val	conf	=	new	SparkConf().setAppName("Simple jdbc application")
    val	sc	=	new	SparkContext(conf)
    val	sqlContext	=	new	SQLContext(sc)
    val spark = sqlContext.sparkSession
    
    val url = "jdbc:sqlserver://localhost:1433;databaseName=test"

    val connectionProperties = new Properties()
    connectionProperties.put("user", "admin")
    connectionProperties.put("password", "12345678")
    
    val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    connectionProperties.setProperty("Driver", driverClass)

    val where = "(select top 10 * from epic.participant_hist) as subset"

    // This import is needed to use the $-notation
    import spark.implicits._

    val df = sqlContext.read.jdbc(url, where, connectionProperties)
    
    // Print the schema in a tree format
    df.printSchema()
    
    println("Selecting... ")
    
    // df.select($"id", $"processState").show()
    
    /*
    // views
    df.createOrReplaceTempView("participants")

    val participantsDF = spark.sql("select firstName from participants")
    
    participantsDF.show()
     
    // filter
    df.filter(df("age") > 21).show()
    */
    
    val ds = df.as[ParticipantHist]
    
    val ds1 = ds.filter("context is not null")
    
    /*
    column select
      From a symbol: 'name
      From a string: $"name" or col(name)
      From an expression: expr("nvl(name, 'unknown') as renamed")
    */
    // ds1.select('id, 'from_store).show()
    ds1.selectExpr("id", "from_store", "array(mrn) as related").show()
    
    
    /*
    df.write
      .option("header", "true")
      .csv("/var/out.csv")
    */
    
  }
}

/*
create table kv (container binary(16), ty varchar(50), ref binary(16), primary key (container, ty, ref));
insert kv select 0xBD2108171260B04FB8191272954E7F40, 'type1', convert(binary(16),newid());
insert kv select 0xBD2108171260B04FB8191272954E7F40, 'type1', convert(binary(16),newid());
insert kv select 0xBD2108171260B04FB8191272954E7F40, 'type1', convert(binary(16),newid());
insert kv select 0xFF1E726082D8A84AAEEFFF69B391BC40, 'type1', convert(binary(16),newid());
insert kv select 0xFF1E726082D8A84AAEEFFF69B391BC40, 'type1', convert(binary(16),newid());
insert kv select 0xFF1E726082D8A84AAEEFFF69B391BC40, 'type1', convert(binary(16),newid());
*/

/*

def	main(args:	Array[String])	{
val	conf	=	new	SparkConf().setAppName("Simple	Application")
val	sc	=	new	SparkContext(conf)
val	sqlContext	=	new	SQLContext(sc)
val	df	=	sqlContext.read.format("jdbc").
option("url",	"jdbc:mysql://statsdb02p-am-tor02:3306/aminno").
option("driver",	"com.mysql.jdbc.Driver").
option("dbtable",	"member").
option("user",	System.getenv("MYSQL_USERNAME")).
option("password",	System.getenv("MYSQL_PASSWORD")).
option("partitionColumn",	"hp").
option("lowerBound",	"0").
option("upperBound",	"44000000").
option("numPartitions",	"5").
load()


				df.registerTempTable("achat")
				val	someRows	=	sqlContext.sql("select	hp,	count(distinct	up)	as	cnt	from	achat	group	by	hp	order	by	cnt	desc").head()
				println("--------see	here!------->"	+	someRows.mkString("	"))
		}
*/


/*

spark.read.format("jdbc").
option("url", "jdbc:mysql://dbhost/sbschhema").
option("dbtable", "mytable").
option("user", "myuser").
option("password", "mypassword").
load().write.parquet("/data/out")



val url="jdbc:mysql://localhost:3306/hadoopdb"

val prop = new java.util.Properties
prop.setProperty("user",”hduser”)
prop.setProperty("password","********")

val people = sqlContext.read.jdbc(url,"person",prop)

people.show 


*/

/*

def main(args: Array[String]){

// parsing input parameters ...
val primaryKey = executeQuery(url, user, password, s"SHOW KEYS FROM ${config("schema")}.${config("table")} WHERE Key_name = 'PRIMARY'").getString(5)
val result = executeQuery(url, user, password, s"select min(${primaryKey}), max(${primaryKey}) from ${config("schema")}.${config("table")}")
    val min = result.getString(1).toInt
    val max = result.getString(2).toInt
    val numPartitions = (max - min) / 5000 + 1
val spark = SparkSession.builder().appName("Spark reading jdbc").getOrCreate()
var df = spark.read.format("jdbc").
option("url", s"${url}${config("schema")}").
option("driver", "com.mysql.jdbc.Driver").
option("lowerBound", min).
option("upperBound", max).
option("numPartitions", numPartitions).
option("partitionColumn", primaryKey).
option("dbtable", config("table")).
option("user", user).
option("password", password).load()
// some data manipulations here ...
df.repartition(10).write.mode(SaveMode.Overwrite).parquet(outputPath)      
}
* */
