import java.text.SimpleDateFormat

import datatypes.{DeparturesArrivalsAggResult, TripEvent}
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
      if (accumulator.day.isEmpty) value.day else accumulator.day,
      if (value.startStop == 1) accumulator.arrivalsCnt + 1 else accumulator.arrivalsCnt,
      if (value.startStop == 0) accumulator.departuresCnt + 1 else accumulator.departuresCnt,
      if (value.startStop == 1) accumulator.arrivingPeopleCnt + value.passengerCount else accumulator.arrivingPeopleCnt,
      if (value.startStop == 0) accumulator.departingPeopleCnt + value.passengerCount else accumulator.departingPeopleCnt
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
      a.arrivalsCnt + b.arrivalsCnt,
      a.departuresCnt + b.departuresCnt,
      a.arrivingPeopleCnt + b.arrivingPeopleCnt,
      a.departingPeopleCnt + b.departingPeopleCnt
    )
}