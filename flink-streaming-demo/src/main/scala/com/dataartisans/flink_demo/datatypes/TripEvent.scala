package com.dataartisans.flink_demo.datatypes

case class TripEvent (
 event_type: Integer,
 event_id: Integer,
 vendor_id: Integer,
 datetime: String,
 longitude: Double,
 latitude: Double,
 payment_type: Integer,
 trip_distance: Double,
 passenger_count: Integer,
 tolls_amount: Double,
 tip_amount: Double,
 total_amount: Double)