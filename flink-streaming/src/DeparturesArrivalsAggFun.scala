import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction

class DeparturesArrivalsAggFun extends AggregateFunction[
  TripEvent,
  DeparturesArrivalsAggResult,
  DeparturesArrivalsAggResult] {
  override def createAccumulator(): DeparturesArrivalsAggResult = DeparturesArrivalsAggResult("", "", "", 0, 0, 0, 0)

  override def add(value: TripEvent,
                   accumulator: DeparturesArrivalsAggResult): DeparturesArrivalsAggResult =
    DeparturesArrivalsAggResult(
      if (accumulator.hour.isEmpty) new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").
        parse(value.timestamp).
        formatted("%tk") else accumulator.hour,
      if (accumulator.borough.isEmpty) value.borough else accumulator.borough,
      if (accumulator.day.isEmpty) new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").parse(value.timestamp).formatted("%tF") else accumulator.day,
      if (value.start_stop == 1) accumulator.arrivals_count + 1 else accumulator.arrivals_count,
      if (value.start_stop == 0) accumulator.departures_count + 1 else accumulator.departures_count,
      if (value.start_stop == 1) accumulator.arriving_ppl_count + value.passenger_count else accumulator.arriving_ppl_count,
      if (value.start_stop == 0) accumulator.departing_ppl_count + value.passenger_count else accumulator.departing_ppl_count
    )

  override def getResult(accumulator: DeparturesArrivalsAggResult): DeparturesArrivalsAggResult = {
    accumulator
  }

  override def merge(a: DeparturesArrivalsAggResult,
                     b: DeparturesArrivalsAggResult): DeparturesArrivalsAggResult =
    DeparturesArrivalsAggResult(
      a.hour,
      a.borough,
      a.day,
      a.arrivals_count + b.arrivals_count,
      a.departures_count + b.departures_count,
      a.arriving_ppl_count + b.arriving_ppl_count,
      a.departing_ppl_count + b.departing_ppl_count
    )
}