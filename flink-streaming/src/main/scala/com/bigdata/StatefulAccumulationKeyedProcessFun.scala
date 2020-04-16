package com.bigdata

import datatypes.DeparturesArrivalsAggResult
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

class StatefulAccumulationKeyedProcessFun extends KeyedProcessFunction[String, DeparturesArrivalsAggResult, DeparturesArrivalsAggResult] {

  private var g_arrivals_count: ValueState[Int] = _
  private var g_departures_count: ValueState[Int] = _
  private var g_arriving_ppl_count: ValueState[Int] = _
  private var g_departing_ppl_count: ValueState[Int] = _

  override def processElement(i: DeparturesArrivalsAggResult, context: KeyedProcessFunction[String, DeparturesArrivalsAggResult, DeparturesArrivalsAggResult]#Context, collector: Collector[DeparturesArrivalsAggResult]): Unit = {

    val new_arrivals_count = g_arrivals_count.value + i.arrivalsCnt.toInt
    val new_departures_count = g_departures_count.value + i.departuresCnt.toInt
    val new_arriving_ppl_count = g_arriving_ppl_count.value + i.arrivingPeopleCnt.toInt
    val new_departing_ppl_count = g_departing_ppl_count.value + i.departingPeopleCnt.toInt

    g_arrivals_count.update(new_arrivals_count)
    g_departures_count.update(new_departures_count)
    g_arriving_ppl_count.update(new_arriving_ppl_count)
    g_departing_ppl_count.update(new_departing_ppl_count)

    collector.collect(DeparturesArrivalsAggResult(
      i.hour,
      i.borough,
      i.day,
      new_arrivals_count,
      new_departures_count,
      new_arriving_ppl_count,
      new_departing_ppl_count
    ))
  }

  override def open(parameters: Configuration): Unit = {
    g_arrivals_count = getRuntimeContext.getState(
      new ValueStateDescriptor[(Int)]("g_arrivals_count", createTypeInformation[(Int)])
    )
    g_departures_count = getRuntimeContext.getState(
      new ValueStateDescriptor[(Int)]("g_departures_count", createTypeInformation[(Int)])
    )
    g_arriving_ppl_count = getRuntimeContext.getState(
      new ValueStateDescriptor[(Int)]("g_arriving_ppl_count", createTypeInformation[(Int)])
    )
    g_departing_ppl_count = getRuntimeContext.getState(
      new ValueStateDescriptor[(Int)]("g_departing_ppl_count", createTypeInformation[(Int)])
    )
  }
}