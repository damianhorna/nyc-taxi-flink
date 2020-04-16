package com.bigdata

import java.text.SimpleDateFormat
import java.util.Date

import datatypes.{DeparturesArrivalsAggResult, TripEvent}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class DepArrProcWindFun extends ProcessWindowFunction[TripEvent, DeparturesArrivalsAggResult, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[TripEvent], out: Collector[DeparturesArrivalsAggResult]): Unit = {
    val dfH = new SimpleDateFormat("H")
    val dfDd = new SimpleDateFormat("yyyy-MM-dd")

    var arrCnt = 0
    var depCnt = 0
    var arrPplCnt = 0
    var depPplCnt = 0

    for( el <- elements){
      if (el.startStop == 0){
        depCnt += 1
        depPplCnt += el.passengerCount
      } else {
        arrCnt += 1
        arrPplCnt += el.passengerCount
      }
    }

    val acc = DeparturesArrivalsAggResult(
      dfH.format(new Date(context.window.getStart)),
      key.split(":")(0),
      dfDd.format(new Date(context.window.getStart)),
      arrCnt,
      depCnt,
      arrPplCnt,
      depPplCnt
    )

    out.collect(acc)
  }
}