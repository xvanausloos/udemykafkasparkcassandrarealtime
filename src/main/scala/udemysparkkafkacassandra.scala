import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}



/*
Udemy https://www.udemy.com/course/apache-kafka-spark-cassandra-real-time-streaming-project/learn/lecture/36728470#questions/20028148
Example for Spark streaming Kakfa input : mytopic
Cassandra output :


 */
object udemysparkkafkacassandra {

  // write dataframe stream to Cassandra

  def writeToCassandra(writeDF: DataFrame, epochId:Long): Unit = {
    writeDF.write
      .format("org.apache.spark.sql.cassandra")
      .option("table","got_char_data")
      .option("keyspace","got_db")
      //.mode("append")
      .save()
  }

  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession
        .builder
        .master("local[*]")
        .appName("kafka_cassandra")
        .enableHiveSupport()
        .getOrCreate()

    spark.conf.set("spark.sql.adaptive.enabled",false)
    spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation",true)
    spark.conf.set("spark.cassandra.connection.host","localhost")


    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","127.0.0.1:9092")
      .option("subscribe","json_topic")
      .option("startingOffsets","earliest")
      .load()

    df.printSchema()

    // Spark Streaming Write to Console
    // Since the value is in binary
    // first we need to convert the binary value to String using selectExpr()
    val personStringDF = df.selectExpr("CAST(value AS STRING)")

    val schema = new StructType()
      .add("charactername", StringType)
      .add("actorname", StringType)
      .add("housename", StringType)
      .add("nickname", StringType)

    val personDF = personStringDF.select(from_json(col("value"), schema).as("data"))
      .select("data.*")

    // write stream to the console
    /*personDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()*/

    // write stream to Cassandra
    personDF.writeStream
      .trigger(ProcessingTime("5 seconds"))
      .outputMode("append")
      .format("com.datastax.spark.connector ")
      .option("table", "got_char_data")
      .option("keyspace", "got_db")
      .start()//replace arguments by _
  }
}