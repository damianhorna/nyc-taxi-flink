import scala.math.max
import java.text.SimpleDateFormat

import datatypes.TripEvent
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class BoundedOutOfOrdernessGenerator extends AssignerWithPeriodicWatermarks[TripEvent] {
  def getTimestamp(s: String): Long = {
    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    format.parse(s).getTime
  }

  val maxOutOfOrderness = 6000L // 6 seconds
  var currentMaxTimestamp: Long = _

  override def extractTimestamp(te: TripEvent, previousElementTimestamp: Long): Long = {
    val timestamp = getTimestamp(te.timestamp)
    currentMaxTimestamp = max(timestamp, currentMaxTimestamp)
    timestamp
  }

  override def getCurrentWatermark: Watermark = {
    new Watermark(currentMaxTimestamp - maxOutOfOrderness)
  }
}