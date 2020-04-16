package datatypes

case class AnomalyAggResult(
                             start: String,
                             stop: String,
                             borough: String,
                             arrivingPeopleCnt: Int,
                             departingPeopleCnt: Int,
                             diff: Int)
