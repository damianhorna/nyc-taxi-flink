package datatypes

case class TripEvent (
                       tripId: Integer,
                       startStop: Integer,
                       timestamp: String,
                       day: String,
                       locationId: Integer,
                       borough: String,
                       passengerCount: Integer,
                       tripDistance: Double,
                       paymentType: Integer,
                       amount: Double,
                       vendorId: Integer)
