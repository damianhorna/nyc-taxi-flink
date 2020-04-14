import java.text.SimpleDateFormat
import java.util.Properties

import datatypes.{AnomalyAggResult, DeparturesArrivalsAggResult, TripEvent}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import utils.CsvReader

import scala.collection.mutable

object DeparturesArrivalsCount {

  /** Main program method */
  def main(args: Array[String]): Unit = {

    val boroughLookup: mutable.Map[String, (_, _, _)] = new CsvReader().readCsv()
    val noOfRetries = 3
    val D = args(1)
    val L = args(3)
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val startDate = sdf.parse("2018-11-01")
    val endDate = sdf.parse("2018-12-31")

    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(noOfRetries, 0))
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
        map(arr => TripEvent(arr(0).toInt, arr(1).toInt,
          arr(2), new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").parse(arr(2)).formatted("%tF"),
          arr(3).toInt, boroughLookup(arr(3))._1.toString,
          arr(4).toInt, arr(5).toDouble, arr(6).toInt,
          arr(7).toDouble, arr(8).toInt
        )).filter(te => (sdf.parse(te.day).compareTo(startDate) >= 0) && sdf.parse(te.day).compareTo(endDate) <= 0)

    val wTaWTripEventsDS: DataStream[TripEvent] = tripEventsDS.
      assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator())

    //    wTaWTripEventsDS.print().setParallelism(1)
    val finalDS: DataStream[DeparturesArrivalsAggResult] = wTaWTripEventsDS.
      keyBy(te => te.borough + ":" + te.day).
      window(TumblingEventTimeWindows.of(Time.hours(1))).
      aggregate(new DeparturesArrivalsAggFun).
      keyBy(mar => mar.borough + mar.day).
      process(new StatefulAccumulationKeyedProcessFun)

    finalDS.print().setParallelism(1)

    val anomalyDS: DataStream[AnomalyAggResult] = wTaWTripEventsDS.
      keyBy(te => te.borough + te.day).
      window(TumblingEventTimeWindows.of(Time.hours(1))).
      aggregate(new DeparturesArrivalsAggFun).
      keyBy(mar => mar.borough).
      window(SlidingEventTimeWindows.of(Time.hours(D.toInt), Time.hours(1))).
      process(new AnomalyProcessWindowFun).
      filter(aag => aag.diff > L.toInt)

    //    anomalyDS.print().setParallelism(1)

    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"))

    val esSinkBuilder = new ElasticsearchSink.Builder[DeparturesArrivalsAggResult](
      httpHosts,
      new ElasticSystemImageSink
    )

    esSinkBuilder.setBulkFlushMaxActions(1)

    // finally, build and add the sink to the job's pipeline
    //    finalDS.addSink(esSinkBuilder.build)


    env.execute("Socket Window WordCount")
  }

  /** Data type for words with count */
  case class WordWithCount(word: String, count: Long)

}

class ElasticSystemImageSink extends ElasticsearchSinkFunction[DeparturesArrivalsAggResult] {
  def process(element: DeparturesArrivalsAggResult, ctx: RuntimeContext, indexer: RequestIndexer) {
    val json = new java.util.HashMap[String, Any]
    json.put("hour", element.hour)
    json.put("borough", element.borough)
    json.put("day", element.day)
    json.put("arrivals_count", element.arrivalsCnt)
    json.put("departures_count", element.departuresCnt)
    json.put("arriving_people_count", element.arrivingPeopleCnt)
    json.put("departing_people_count", element.departingPeopleCnt)

    val rqst: IndexRequest = Requests.indexRequest
      .index("nyc-index")
      .source(json)

    indexer.add(rqst)
  }
}