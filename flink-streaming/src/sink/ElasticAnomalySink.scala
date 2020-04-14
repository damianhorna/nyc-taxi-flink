package sink

import java.text.SimpleDateFormat

import datatypes.{AnomalyAggResult, DeparturesArrivalsAggResult}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

class ElasticAnomalySink extends ElasticsearchSinkFunction[AnomalyAggResult] {
  def process(element: AnomalyAggResult, ctx: RuntimeContext, indexer: RequestIndexer) {
    val json = new java.util.HashMap[String, Any]
    json.put("start", new SimpleDateFormat("yyyy-MM-dd H").parse(element.start))
    json.put("stop", new SimpleDateFormat("yyyy-MM-dd H").parse(element.stop))
    json.put("arriving_people_count", element.arrivingPeopleCnt)
    json.put("departing_people_count", element.departingPeopleCnt)
    json.put("diff", element.diff)

    val rqst: IndexRequest = Requests.indexRequest
      .index("nyc-anomaly")
      .source(json)

    indexer.add(rqst)
  }
}