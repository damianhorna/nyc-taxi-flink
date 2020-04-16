package com.bigdata

import java.text.SimpleDateFormat
import java.util.Date

import datatypes.{AnomalyAggResult, DeparturesArrivalsAggResult}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class AnomalyProcessWindowFun extends ProcessWindowFunction[DeparturesArrivalsAggResult, AnomalyAggResult, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[DeparturesArrivalsAggResult], out: Collector[AnomalyAggResult]): Unit = {
    val df = new SimpleDateFormat("yyyy-MM-dd H")

    var arrPplCnt = 0
    var depPplCnt = 0

    for (el <- elements) {
      arrPplCnt += el.arrivingPeopleCnt
      depPplCnt += el.departingPeopleCnt
    }

    val acc = AnomalyAggResult(
      df.format(new Date(context.window.getStart)),
      df.format(new Date(context.window.getEnd)),
      key,
      arrPplCnt,
      depPplCnt,
      depPplCnt - arrPplCnt)

    out.collect(acc)
  }
}