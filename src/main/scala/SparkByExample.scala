import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger


/*
https://sparkbyexamples.com/spark/spark-streaming-with-kafka/
-Create a topic json_topic
-Run kafka-topics --create --replication-factor 1 --partitions 1 --topic json_topic --bootstrap-server localhost:9092



 */
object SparkByExample {

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
      .add("id", IntegerType)
      .add("firstname", StringType)
      .add("middlename", StringType)
      .add("lastname", StringType)
      .add("dob_year", IntegerType)
      .add("dob_month", IntegerType)
      .add("gender", StringType)
      .add("salary", IntegerType)

    val personDF = personStringDF.select(from_json(col("value"), schema).as("data"))
      .select("data.*")


    personDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }
}