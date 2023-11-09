import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger


/*
https://sparkbyexamples.com/spark/spark-streaming-with-kafka/
Create a topic json_topic

 */
object KafkaCassandraSpark {

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

    def writeToCassandra(writeDF: DataFrame, epochId:Long): Unit = {
      writeDF
        .write
        .format("org.apache.spark.sql.cassandra")
        .option("table","got_char_data")
        .option("keyspace","got_db")
        .mode("append")
        .save()
    }

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","127.0.0.1:9092")
      .option("subscribe","json_topic")
      .option("startingOffsets","earliest")
      .load()

    val personStringDF = df.selectExpr("CAST(value AS STRING)")

    val schema = new StructType()
      .add("charactername", StringType)
      .add("actorname", StringType)
      .add("housename", StringType)
      .add("nickname", StringType)

    val personDF = personStringDF.select(from_json(col("value"), schema).as("data"))
      .select("data.*")

    personDF.writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode("append")
      .foreachBatch(writeToCassandra _)
      .start()
      .awaitTermination()
  }
}