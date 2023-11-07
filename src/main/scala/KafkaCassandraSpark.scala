import org.apache.spark.sql.SparkSession

object KafkaCassandraSpark {

  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder.master("local[*]").appName("kafka_cassandra").enableHiveSupport().getOrCreate()
  }

}
