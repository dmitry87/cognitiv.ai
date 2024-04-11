package ai.cognitiv.nyc.taxi.reader.cognitiv.reporting

import ai.cognitiv.nyc.taxi.reader.cognitiv.model.{AggregatedTripData, AnalyzedTripRecord}

import java.sql.Timestamp

trait Reportings {

    def toAggregatedTripData(paymentType: Double, minFare: Double, maxFare: Double, count: Long, fareSum: Double, tollsSum: Double): AggregatedTripData = {
        val t = new AggregatedTripData()
        t.setPaymentType(paymentType)
        t.setMinFare(minFare)
        t.setMaxFare(maxFare)
        t.setCount(count)
        t.setFareSum(fareSum)
        t.setTollsSum(tollsSum)
        t
    }

    def toAnalyzedTripRecord(vendorId: Long, pickUp: Timestamp, dropOff: Timestamp, fareAmount: Double, tollAmount: Double, paymentType: Double,
                             pickUpLocationId: Option[Long] = None, dropOffLocationId: Option[Long] = None, color: Option[String] = None): AnalyzedTripRecord = {
        val t = new AnalyzedTripRecord()
        t.setVendorId(vendorId)
        t.setPickUpDateTime(pickUp)
        t.setDropOffDateTime(dropOff)
        t.setFareAmount(fareAmount)
        t.setTollAmount(tollAmount)
        t.setPaymentType(paymentType)
        pickUpLocationId.foreach(t.setPickUpLocationId(_))
        dropOffLocationId.foreach(t.setDropOffLocationId(_))
        color.foreach(t.setColor)

        t
    }

}
