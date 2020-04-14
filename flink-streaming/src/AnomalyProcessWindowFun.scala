import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class AnomalyProcessWindowFun extends ProcessWindowFunction[DeparturesArrivalsAggResult, AnomalyAggResult, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[DeparturesArrivalsAggResult], out: Collector[AnomalyAggResult]): Unit = {
    val df = new SimpleDateFormat("yyyy-MM-dd H")

    var arr_ppl_count = 0
    var dep_ppl_count = 0

    for (el <- elements) {
      arr_ppl_count += el.arriving_ppl_count
      dep_ppl_count += el.departing_ppl_count
    }

    val acc = AnomalyAggResult(
      df.format(new Date(context.window.getStart)),
      df.format(new Date(context.window.getEnd)),
      key,
      arr_ppl_count,
      dep_ppl_count,
      dep_ppl_count - arr_ppl_count)

    out.collect(acc)
  }
}