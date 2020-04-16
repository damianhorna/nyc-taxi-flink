package datatypes

case class DeparturesArrivalsAggResult(
                                        hour: String,
                                        borough: String,
                                        day: String,
                                        arrivalsCnt: Integer,
                                        departuresCnt: Integer,
                                        arrivingPeopleCnt: Integer,
                                        departingPeopleCnt: Integer)
