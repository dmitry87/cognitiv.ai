package ai.cognitiv.nyc.taxi.reader.cognitiv.read

import ai.cognitiv.nyc.taxi.reader.cognitiv.ImplicitHelpers._
import ai.cognitiv.nyc.taxi.reader.cognitiv.model.{GreenTripRecord, YellowTripRecord}

import java.sql.Timestamp
import java.time.LocalDateTime


trait Readings {

    def yellowTrip(
                      vendorID: Option[Long],
                      tpepPickUpDatetime: Option[String],
                      tpepDropOffDatetime: Option[String],
                      passengerCount: Option[Double],
                      tripDistance: Option[Double],
                      rateCodeId: Option[Double],
                      storeAndFwdFlag: Option[String],
                      pickUpLocationId: Option[Long],
                      dropOffLocationId: Option[Long],
                      paymentType: Option[Long],
                      fareAmount: Option[Double],
                      extra: Option[Double],
                      mtaTax: Option[Double],
                      tipAmount: Option[Double],
                      tollsAmount: Option[Double],
                      improvementSurcharge: Option[Double],
                      totalAmount: Option[Double],
                      congestionSurcharge: Option[Double],
                      airportFee: Option[Double],
                  ): YellowTripRecord = {
        val t = new YellowTripRecord()
        vendorID.foreach(t.setVendorID(_))
        tpepPickUpDatetime.map(LocalDateTime.parse).map(Timestamp.valueOf).foreach(t.setTpepPickUpDatetime(_))
        tpepDropOffDatetime.map(LocalDateTime.parse).map(Timestamp.valueOf).foreach(t.setTpepDropOffDatetime(_))
        passengerCount.foreach(t.setPassengerCount(_))
        tripDistance.foreach(t.setTripDistance(_))
        rateCodeId.foreach(t.setRateCodeId(_))
        storeAndFwdFlag.foreach(t.setStoreAndFwdFlag(_))
        pickUpLocationId.foreach(t.setPickUpLocationId(_))
        dropOffLocationId.foreach(t.setDropOffLocationId(_))
        paymentType.foreach(t.setPaymentType(_))
        fareAmount.foreach(t.setFareAmount(_))
        extra.foreach(t.setExtra(_))
        mtaTax.foreach(t.setMtaTax(_))
        tipAmount.foreach(t.setTipAmount(_))
        tollsAmount.foreach(t.setTollsAmount(_))
        improvementSurcharge.foreach(t.setImprovementSurcharge(_))
        totalAmount.foreach(t.setTotalAmount(_))
        congestionSurcharge.foreach(t.setCongestionSurcharge(_))
        airportFee.foreach(t.setAirportFee(_))

        t
    }

    def greenTrip(
                     vendorId: Option[Long],
                     lpepPickUpDatetime: Option[String],
                     lpepDropOffDatetime: Option[String],
                     storeAndFwdFlag: Option[String],
                     ratecodeID: Option[Double],
                     pickUpLocationId: Option[Long],
                     dropOffLocationId: Option[Long],
                     passengerCount: Option[Double],
                     tripDistance: Option[Double],
                     fareAmount: Option[Double],
                     extra: Option[Double],
                     mtaTax: Option[Double],
                     tipAmount: Option[Double],
                     tollsAmount: Option[Double],
                     ehailFee: Option[Double],
                     improvementSurcharge: Option[Double],
                     totalAmount: Option[Double],
                     paymentType: Option[Double],
                     tripType: Option[Double],
                 ): GreenTripRecord = {
        val t = new GreenTripRecord
        vendorId.foreach(t.setVendorId(_))
        lpepPickUpDatetime.map(LocalDateTime.parse(_)).map(Timestamp.valueOf).foreach(t.setLpepPickUpDatetime)
        lpepDropOffDatetime.map(LocalDateTime.parse(_)).map(Timestamp.valueOf).foreach(t.setLpepDropOffDatetime)
        storeAndFwdFlag.foreach(t.setStoreAndFwdFlag)
        ratecodeID.foreach(t.setRatecodeID(_))
        pickUpLocationId.foreach(t.setPickUpLocationId(_))
        dropOffLocationId.foreach(t.setDropOffLocationId(_))
        passengerCount.foreach(t.setPassengerCount(_))
        tripDistance.foreach(t.setTripDistance(_))
        fareAmount.foreach(t.setFareAmount(_))
        extra.foreach(t.setExtra(_))
        mtaTax.foreach(t.setMtaTax(_))
        tipAmount.foreach(t.setTipAmount(_))
        tollsAmount.foreach(t.setTollsAmount(_))
        ehailFee.foreach(t.setEhailFee(_))
        improvementSurcharge.foreach(t.setImprovementSurcharge(_))
        totalAmount.foreach(t.setTotalAmount(_))
        paymentType.foreach(t.setPaymentType(_))
        tripType.foreach(t.setTripType(_))

        t
    }

    def yellowSample(): YellowTripRecord = yellowTrip(
        vendorID = 1L.toOpt,
        tpepPickUpDatetime = "2023-09-30T17:16:44.00".toOpt,
        tpepDropOffDatetime = "2023-09-30T17:16:49.00".toOpt,
        passengerCount = 1D.toOpt,
        tripDistance = 0D.toOpt,
        rateCodeId = 1D.toOpt,
        storeAndFwdFlag = "N".toOpt,
        pickUpLocationId = 168L.toOpt,
        dropOffLocationId = 168L.toOpt,
        paymentType = 2L.toOpt,
        fareAmount = 3D.toOpt,
        extra = 1D.toOpt,
        mtaTax = 0.5.toOpt,
        tipAmount = 0D.toOpt,
        tollsAmount = 0D.toOpt,
        improvementSurcharge = 1D.toOpt,
        totalAmount = 5.5.toOpt,
        congestionSurcharge = 0D.toOpt,
        airportFee = 0D.toOpt
    )

    def greenSample(): GreenTripRecord = greenTrip(
        vendorId = 2L.toOpt,
        lpepPickUpDatetime = "2023-09-30T17:57:33.00".toOpt,
        lpepDropOffDatetime = "2023-09-30T18:07:58.00".toOpt,
        storeAndFwdFlag = "N".toOpt,
        ratecodeID = 1D.toOpt,
        pickUpLocationId = 166L.toOpt,
        dropOffLocationId = 74L.toOpt,
        passengerCount = 1D.toOpt,
        tripDistance = 1.45.toOpt,
        fareAmount = 12.1.toOpt,
        extra = 1.0.toOpt,
        mtaTax = 0.5.toOpt,
        tipAmount = 2.92.toOpt,
        tollsAmount = 0D.toOpt,
        ehailFee = None,
        improvementSurcharge = 1D.toOpt,
        totalAmount = 17.52.toOpt,
        paymentType = 1D.toOpt,
        tripType = 1D.toOpt
    )

}
