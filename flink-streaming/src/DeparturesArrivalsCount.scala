import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import scala.collection.mutable

object DeparturesArrivalsCount {

  def read_csv(): mutable.Map[String, (_, _, _)] = {
    var boroughLookup = mutable.Map[String, (_, _, _)]()

    println("Reading csv file")
    val bufferedSource = io.Source.fromFile("/home/dhorna/dev/studies/mgr-sem1/bd/nyc-taxi-flink/data/taxi_zone_lookup.csv")
    for (line <- bufferedSource.getLines) {
      val Array(locationId, borough, zone, serviceZone) = line.split(",").map(_.trim)
      boroughLookup += (locationId -> (borough, zone, serviceZone))
    }
    boroughLookup.-("LocationID")
    bufferedSource.close
    boroughLookup
  }

  /** Main program method */
  def main(args: Array[String]): Unit = {

    val boroughLookup: mutable.Map[String, (_, _, _)] = read_csv()
    val no_of_retries = 3
    val D = args(1)
    val L = args(3)

    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(no_of_retries, 0))
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "testGroup")

    val text: DataStream[String] = env
      .addSource(new FlinkKafkaConsumer[String]("testTopic", new SimpleStringSchema(), properties))

    //    text.print().setParallelism(1)

    val tripEventsDS: org.apache.flink.streaming.api.scala.DataStream[TripEvent] =
      text.filter(s => !s.startsWith("event_type")).
        map(_.split(",")).
        filter(_.length == 9).
        filter(arr => boroughLookup.keySet.contains(arr(3))).
        map(arr => TripEvent(
          arr(0).toInt,
          arr(1).toInt,
          arr(2),
          new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").parse(arr(2)).formatted("%tF"),
          arr(3).toInt,
          boroughLookup(arr(3))._1.toString,
          arr(4).toInt,
          arr(5).toDouble,
          arr(6).toInt,
          arr(7).toDouble,
          arr(8).toInt
        ))

    val wTaWTripEventsDS: DataStream[TripEvent] = tripEventsDS.
      assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator())

    //    wTaWTripEventsDS.print().setParallelism(1)
    val finalDS: DataStream[DeparturesArrivalsAggResult] = wTaWTripEventsDS.
      keyBy(te => te.borough +":"+ te.day).
      window(TumblingEventTimeWindows.of(Time.hours(1))).
      process(new DepArrProcWindFun).
      keyBy(mar => mar.borough + mar.day).
      process(new StatefulAccumulationKeyedProcessFun)

    finalDS.print().setParallelism(1)

//    val anomalyDS =  wTaWTripEventsDS.
//      keyBy(te => te.borough + te.day).
//      window(TumblingEventTimeWindows.of(Time.hours(1))).
//      aggregate(new DeparturesArrivalsAggFun).
//      keyBy(mar => mar.borough).
//      window(SlidingEventTimeWindows.of(Time.hours(D.toInt), Time.hours(1))).
//      process(new AnomalyProcessWindowFun)
//
//    anomalyDS.print().setParallelism(1)


    env.execute("Socket Window WordCount")
  }

  /** Data type for words with count */
  case class WordWithCount(word: String, count: Long)

}