import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import scala.collection.mutable

/**
 * Implements a streaming windowed version of the "WordCount" program.
 *
 * This program connects to a server socket and reads strings from the socket.
 * The easiest way to try this out is to open a text sever (at port 12345)
 * using the ''netcat'' tool via
 * {{{
 * nc -l 12345 on Linux or nc -l -p 12345 on Windows
 * }}}
 * and run this example with the hostname and the port as arguments..
 */
object SocketWindowWordCount {

  def read_csv(): mutable.Map[String, (_, _, _)] = {
    var lookup = mutable.Map[String, (_, _, _)]()

    println("Reading csv file")
    val bufferedSource = io.Source.fromFile("/home/dhorna/dev/studies/mgr-sem1/bd/nyc-taxi-flink/data/taxi_zone_lookup.csv")
    for (line <- bufferedSource.getLines) {
      val Array(locationId, borough, zone, serviceZone)  = line.drop(1).split(",").map(_.trim)
      lookup += (locationId -> (borough, zone, serviceZone))
    }
    bufferedSource.close
    lookup
  }

  /** Main program method */
  def main(args: Array[String]) : Unit = {

    var lookup = read_csv()

    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "testGroup")

    var text : DataStream[String] = env
      .addSource(new FlinkKafkaConsumer[String]("testTopic", new SimpleStringSchema(), properties))

    text.rebalance.map(s => s).print().setParallelism(1)

    env.execute("Socket Window WordCount")
  }

  /** Data type for words with count */
  case class WordWithCount(word: String, count: Long)
}