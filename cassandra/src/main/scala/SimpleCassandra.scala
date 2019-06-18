import com.datastax.spark.connector._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.cassandra._

object SimpleApp {
  def main(args: Array[String]) {

  val conf: SparkConf = new SparkConf().setAppName("Simple Cassandra") // .setMaster("local")
	val sc: SparkContext = new SparkContext(conf)
  
  // Loading and analyzing data from Cassandra
  val rdd = sc.cassandraTable("test", "kv")
  println(rdd.count)
  println(rdd.first)
  println(rdd.map(_.getInt("value")).sum)

  // Saving data from RDD to Cassandra
  // Add two more rows to the table:

  val collection = sc.parallelize(Seq(("key3", 3), ("key4", 4)))
  collection.saveToCassandra("test", "kv", SomeColumns("key", "value"))
}

/*
CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
CREATE TABLE test.kv(key text PRIMARY KEY, value int);
 
INSERT INTO test.kv(key, value) VALUES ('key1', 1);
INSERT INTO test.kv(key, value) VALUES ('key2', 2);
*/
