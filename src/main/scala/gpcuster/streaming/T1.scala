package gpcuster.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

object T1 {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.SparkSession

    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.set("spark.sql.streaming.metricsEnabled", "true")
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.default.parallelism", "1")
    conf.set("spark.sql.shuffle.partitions", "1")

    val builder = SparkSession.builder.config(conf)

    val spark = builder
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    import spark.implicits._

    spark.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started: " + queryStarted.id)
      }
      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + queryTerminated.id)
      }
      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        println("Query made progress: " + queryProgress.progress)
      }
    })

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("checkpointLocation", s"/tmp/spark/T1")
      //.trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    query.awaitTermination()
  }
}
