package gpcuster.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

import scala.util.parsing.json.JSONObject

object T3 {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession

    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.set("spark.sql.streaming.metricsEnabled", "true")
    conf.set("spark.default.parallelism", "1")
    conf.set("spark.sql.shuffle.partitions", "1")

    val builder = SparkSession.builder.config(conf)

    val spark = builder
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    import spark.implicits._

    val parser = new JSONParser();
    JSONObject json = (JSONObject) parser.parse(stringToParse);

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

    // Start running the query that prints the running counts to the console
    val query = words.writeStream
      .outputMode("append")
      .format("console")
      .option("checkpointLocation", s"/tmp/spark/T0")
      //.trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    query.awaitTermination()
  }
}
