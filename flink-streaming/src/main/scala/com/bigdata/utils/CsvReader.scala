package utils

import scala.collection.mutable

class CsvReader {
  def readCsv(): mutable.Map[String, (_, _, _)] = {
    var boroughLookup = mutable.Map[String, (_, _, _)]()

    println("Reading csv file")
    val bufferedSource = io.Source.fromFile("/home/dhorna/dev/studies/mgr-sem1/bd/nyc-taxi-flink/data/taxi_zone_lookup.csv")
    for (line <- bufferedSource.getLines) {
      val Array(locationId, borough, zone, serviceZone) = line.split(",").map(_.trim)
      boroughLookup += (locationId -> (borough, zone, serviceZone))
    }
    boroughLookup.-("LocationID")
    bufferedSource.close
    boroughLookup
  }

}
