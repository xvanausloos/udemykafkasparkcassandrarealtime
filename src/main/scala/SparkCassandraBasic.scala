import com.datastax.spark.connector.{SomeColumns, toRDDFunctions}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/*
9.11.23: Spark 3.5.0 and Cassandra 4.1.3
Spark SQL and Cassandra
No streaming here.
https://jentekllc8888.medium.com/tutorial-integrate-spark-sql-and-cassandra-complete-with-scala-and-python-example-codes-8307fe9c2901
It works well.
 */

object SparkCassandraBasic {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("com").setLevel(Level.ERROR)
    val conf = new SparkConf()
    conf.set("spark.master", "local[*]")
    conf.set("spark.app.name", "exampleApp")

    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder
      .appName("SQL example")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/tmp")
      .config("spark.sql.catalog.history", "com.datastax.spark.connector.datasource.CassandraCatalog")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
      .getOrCreate()

    spark.sql("CREATE DATABASE IF NOT EXISTS history.sales WITH DBPROPERTIES (class='SimpleStrategy',replication_factor='1')")

    spark.sql("CREATE TABLE IF NOT EXISTS history.sales.salesfact (key Int, sale_date TIMESTAMP, product STRING, value DOUBLE) USING cassandra PARTITIONED BY (key)") //List all keyspaces

    spark.sql("SHOW NAMESPACES FROM history").show(false)

    //List tables under keyspace sales
    spark.sql("SHOW TABLES FROM history.sales").show(false) //Create some sales records, write them into Cassandra table sales.salesfactspark.createDataFrame(Seq((0,"2020-09-06 10:00:00","TV","200.00"),(1,"2020-09-06 11:00:00","Laptop","500.00"))).toDF("key","sales_date","product","value").rdd.saveToCassandra("sales", "salesfact", SomeColumns("key", "sale_date", "product", "value"))//Query data from Cassandra by Spark SQL, using window function that is not available on CQL

    spark.sql("SELECT product, sum(value) over (partition by product) total_sales FROM history.sales.salesfact").show(false)

    //

    //Create some sales records, write them into Cassandra table sales.salesfact
    spark.createDataFrame(Seq((0,"2020-09-06 10:00:00","TV","200.00"),(1,"2020-09-06 11:00:00","Laptop","500.00"))).toDF("key","sales_date","product","value")
      .rdd.saveToCassandra("sales", "salesfact", SomeColumns("key", "sale_date", "product", "value"))

    //Query data from Cassandra by Spark SQL, using window function that is not available on CQL
    spark.sql("SELECT product, sum(value) over (partition by product) total_sales FROM history.sales.salesfact").show(false)

    sc.stop()
  }
}