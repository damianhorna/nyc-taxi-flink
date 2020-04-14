import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable


class MyProcessFunction extends KeyedProcessFunction[String, MyAggResult, MyAggResult] {

  private var acc: ValueState[Int] = _

  override def processElement(i: MyAggResult, context: KeyedProcessFunction[String, MyAggResult, MyAggResult]#Context, collector: Collector[MyAggResult]): Unit = {
    var tmpAcc = 0
    try{
      tmpAcc = acc.value
    } catch {
      case x: NullPointerException => tmpAcc = 0
    }

    val newAcc = tmpAcc + i.arrivals_count.toInt
    acc.update(newAcc)
    collector.collect(new MyAggResult(
      i.hour,
      i.borough,
      i.day,
      newAcc,
      i.departures_count,
      i.arriving_ppl_count,
      i.departing_ppl_count
    ))
  }

  override def open(parameters: Configuration): Unit = {
    acc = getRuntimeContext.getState(
      new ValueStateDescriptor[(Int)]("acc", createTypeInformation[(Int)])
    )
  }
}

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
    val finalDS: DataStream[MyAggResult] = wTaWTripEventsDS.
      keyBy(te => te.borough + new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").parse(te.timestamp).formatted("%tF")).
      window(TumblingEventTimeWindows.of(Time.hours(1))).
      aggregate(new MyAggFun).
      keyBy(mar => mar.borough + mar.day).
      process(new MyProcessFunction)

    finalDS.print().setParallelism(1)

    env.execute("Socket Window WordCount")
  }

  /** Data type for words with count */
  case class WordWithCount(word: String, count: Long)

}