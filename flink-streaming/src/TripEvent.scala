case class TripEvent (
                       trip_id: Integer,
                       start_stop: Integer,
                       timestamp: String,
                       day: String,
                       location_id: Integer,
                       borough: String,
                       passenger_count: Integer,
                       trip_distance: Double,
                       payment_type: Integer,
                       amount: Double,
                       vendor_id: Integer)