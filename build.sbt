ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
      name := "udemykafkasparkcassandrarealtime"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.2",
  "org.apache.spark" %% "spark-sql" % "3.3.2",
  "org.apache.spark" %% "spark-mllib" % "3.3.2",
  "org.apache.logging.log4j" % "log4j-api" % "2.21.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.21.1",
  // https://mvnrepository.com/artifact/org.apache.spark/spark-hive
  "org.apache.spark" %% "spark-hive" % "3.3.2",
  "com.github.jnr" % "jnr-posix" % "3.0.61",
  "joda-time" % "joda-time" % "2.10.6",
  // https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.4.1",
  "org.apache.spark" %% "spark-streaming" % "3.3.2",
  // https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.3.2",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.3.2",
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.2.0"
)