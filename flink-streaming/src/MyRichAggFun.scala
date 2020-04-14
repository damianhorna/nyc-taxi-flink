
import org.apache.flink.api.common.functions.RichAggregateFunction

class MyRichAggFun extends RichAggregateFunction[
  MyAggResult,
  MyAggResult,
  MyAggResult] {
  override def createAccumulator(): MyAggResult = MyAggResult("", "", "", 0, 0, 0, 0)

  override def add(value: MyAggResult,
                   accumulator: MyAggResult): MyAggResult =
    MyAggResult(
      if (accumulator.hour.isEmpty) value.hour else accumulator.hour,
      if (accumulator.borough.isEmpty) value.borough else accumulator.borough,
      if (accumulator.day.isEmpty) value.day else accumulator.day,
      value.arrivals_count  + accumulator.arrivals_count,
      value.departures_count + accumulator.departures_count,
      value.arriving_ppl_count + accumulator.arriving_ppl_count,
      value.departing_ppl_count + accumulator.departing_ppl_count
    )

  override def getResult(accumulator: MyAggResult): MyAggResult = {
    accumulator
  }

  override def merge(a: MyAggResult,
                     b: MyAggResult): MyAggResult =
    MyAggResult(
      a.hour,
      a.borough,
      a.day,
      a.arrivals_count + b.arrivals_count,
      a.departures_count + b.departures_count,
      a.arriving_ppl_count + b.arriving_ppl_count,
      a.departing_ppl_count + b.departing_ppl_count
    )
}