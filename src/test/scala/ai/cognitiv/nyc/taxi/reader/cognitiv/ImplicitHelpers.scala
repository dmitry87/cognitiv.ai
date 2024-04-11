package ai.cognitiv.nyc.taxi.reader.cognitiv

import ai.cognitiv.nyc.taxi.reader.cognitiv.model.{GreenTripRecord, TaxiColor, YellowTripRecord}

import java.sql.Timestamp
import java.time.LocalDateTime

object ImplicitHelpers {
    implicit class RichVal[T](anyVal: T) {
        def toOpt: Option[T] = Option(anyVal)
    }

    implicit class RichEnumVal(anyVal: TaxiColor) {
        def toOpt: Option[String] = Option(anyVal.getColor)
    }

    implicit class RichYellowTrip(s: YellowTripRecord) {
        def copy(
                    vendorID: Option[Long] = None,
                    tpepPickUpDatetime: Option[String] = None,
                    tpepDropOffDatetime: Option[String] = None,
                    passengerCount: Option[Double] = None,
                    tripDistance: Option[Double] = None,
                    rateCodeId: Option[Double] = None,
                    storeAndFwdFlag: Option[String] = None,
                    pickUpLocationId: Option[Long] = None,
                    dropOffLocationId: Option[Long] = None,
                    paymentType: Option[Long] = None,
                    fareAmount: Option[Double] = None,
                    extra: Option[Double] = None,
                    mtaTax: Option[Double] = None,
                    tipAmount: Option[Double] = None,
                    tollsAmount: Option[Double] = None,
                    improvementSurcharge: Option[Double] = None,
                    totalAmount: Option[Double] = None,
                    congestionSurcharge: Option[Double] = None,
                    airportFee: Option[Double] = None,
                ): YellowTripRecord = {
            val t = new YellowTripRecord
            val tempVendorID: Option[Long] = vendorID.orElse(if (s.getVendorID == null) None else Option(s.getVendorID))
            val tempTpepPickUpDatetime: Option[Timestamp] = tpepPickUpDatetime.map(LocalDateTime.parse(_)).map(Timestamp.valueOf).orElse(if (s.getTpepPickUpDatetime == null) None else Option(s.getTpepPickUpDatetime))
            val tempTpepDropOffDatetime: Option[Timestamp] = tpepDropOffDatetime.map(LocalDateTime.parse(_)).map(Timestamp.valueOf).orElse(if (s.getTpepDropOffDatetime == null) None else Option(s.getTpepDropOffDatetime))
            val tempPassengerCount: Option[Double] = passengerCount.orElse(if (s.getPassengerCount == null) None else Option(s.getPassengerCount.toLong))
            val tempTripDistance: Option[Double] = tripDistance.orElse(if (s.getTripDistance == null) None else Option(s.getTripDistance.doubleValue()))
            val tempRateCodeId: Option[Double] = rateCodeId.orElse(if (s.getRateCodeId == null) None else Option(s.getRateCodeId.doubleValue()))
            val tempStoreAndFwdFlag: Option[String] = storeAndFwdFlag.orElse(if (s.getStoreAndFwdFlag == null) None else Option(s.getStoreAndFwdFlag))
            val tempPickUpLocationId: Option[Long] = pickUpLocationId.orElse(if (s.getPickUpLocationId == null) None else Option(s.getPickUpLocationId.toLong))
            val tempDropOffLocationId: Option[Long] = dropOffLocationId.orElse(if (s.getDropOffLocationId == null) None else Option(s.getDropOffLocationId.toLong))
            val tempPaymentType: Option[Long] = paymentType.orElse(if (s.getPaymentType == null) None else Option(s.getPaymentType.toLong))
            val tempFareAmount: Option[Double] = fareAmount.orElse(if (s.getFareAmount == null) None else Option(s.getFareAmount.doubleValue()))
            val tempExtra: Option[Double] = extra.orElse(if (s.getExtra == null) None else Option(s.getExtra.doubleValue()))
            val tempMtaTax: Option[Double] = mtaTax.orElse(if (s.getMtaTax == null) None else Option(s.getMtaTax.doubleValue()))
            val tempTipAmount: Option[Double] = tipAmount.orElse(if (s.getTipAmount == null) None else Option(s.getTipAmount.doubleValue()))
            val tempTollsAmount: Option[Double] = tollsAmount.orElse(if (s.getTollsAmount == null) None else Option(s.getTollsAmount.doubleValue()))
            val tempImprovementSurcharge: Option[Double] = improvementSurcharge.orElse(if (s.getImprovementSurcharge == null) None else Option(s.getImprovementSurcharge.doubleValue()))
            val tempTotalAmount: Option[Double] = totalAmount.orElse(if (s.getTotalAmount == null) None else Option(s.getTotalAmount.doubleValue()))
            val tempCongestionSurcharge: Option[Double] = congestionSurcharge.orElse(if (s.getCongestionSurcharge == null) None else Option(s.getCongestionSurcharge.doubleValue()))
            val tempAirportFee: Option[Double] = airportFee.orElse(if (s.getAirportFee == null) None else Option(s.getAirportFee.doubleValue()))

            tempVendorID.foreach(t.setVendorID(_))
            tempTpepPickUpDatetime.foreach(t.setTpepPickUpDatetime(_))
            tempTpepDropOffDatetime.foreach(t.setTpepDropOffDatetime(_))
            tempPassengerCount.foreach(t.setPassengerCount(_))
            tempTripDistance.foreach(t.setTripDistance(_))
            tempRateCodeId.foreach(t.setRateCodeId(_))
            tempStoreAndFwdFlag.foreach(t.setStoreAndFwdFlag(_))
            tempPickUpLocationId.foreach(t.setPickUpLocationId(_))
            tempDropOffLocationId.foreach(t.setDropOffLocationId(_))
            tempPaymentType.foreach(t.setPaymentType(_))
            tempFareAmount.foreach(t.setFareAmount(_))
            tempExtra.foreach(t.setExtra(_))
            tempMtaTax.foreach(t.setMtaTax(_))
            tempTipAmount.foreach(t.setTipAmount(_))
            tempTollsAmount.foreach(t.setTollsAmount(_))
            tempImprovementSurcharge.foreach(t.setImprovementSurcharge(_))
            tempTotalAmount.foreach(t.setTotalAmount(_))
            tempCongestionSurcharge.foreach(t.setCongestionSurcharge(_))
            tempAirportFee.foreach(t.setAirportFee(_))

            t
        }
    }

    implicit class RichGreenTrip(s: GreenTripRecord) {
        def copy(
                    vendorId: Option[Long] = None,
                    lpepPickUpDatetime: Option[String] = None,
                    lpepDropOffDatetime: Option[String] = None,
                    storeAndFwdFlag: Option[String] = None,
                    ratecodeID: Option[Double] = None,
                    pickUpLocationId: Option[Long] = None,
                    dropOffLocationId: Option[Long] = None,
                    passengerCount: Option[Double] = None,
                    tripDistance: Option[Double] = None,
                    fareAmount: Option[Double] = None,
                    extra: Option[Double] = None,
                    mtaTax: Option[Double] = None,
                    tipAmount: Option[Double] = None,
                    tollsAmount: Option[Double] = None,
                    ehailFee: Option[Double] = None,
                    improvementSurcharge: Option[Double] = None,
                    totalAmount: Option[Double] = None,
                    paymentType: Option[Double] = None,
                    tripType: Option[Double] = None,
                ): GreenTripRecord = {
            val t = new GreenTripRecord
            val tempVendorId: Option[Long] = vendorId.orElse(if (s.getVendorId() == null) None else Option(s.getVendorId()))
            val tempLpepPickUpDatetime: Option[Timestamp] = lpepPickUpDatetime.map(LocalDateTime.parse).map(Timestamp.valueOf).orElse(if (s.getLpepPickUpDatetime == null) None else Option(s.getLpepPickUpDatetime))
            val tempLpepDropOffDatetime: Option[Timestamp] = lpepDropOffDatetime.map(LocalDateTime.parse).map(Timestamp.valueOf).orElse(if (s.getLpepDropOffDatetime == null) None else Option(s.getLpepDropOffDatetime))
            val tempStoreAndFwdFlag: Option[String] = storeAndFwdFlag.orElse(if (s.getStoreAndFwdFlag == null) None else Option(s.getStoreAndFwdFlag))
            val tempRatecodeID: Option[Double] = ratecodeID.orElse(if (s.getRatecodeID() == null) None else Option(s.getRatecodeID()))
            val tempPickUpLocationId: Option[Long] = pickUpLocationId.orElse(if (s.getPickUpLocationId() == null) None else Option(s.getPickUpLocationId()))
            val tempDropOffLocationId: Option[Long] = dropOffLocationId.orElse(if (s.getDropOffLocationId() == null) None else Option(s.getDropOffLocationId()))
            val tempPassengerCount: Option[Double] = passengerCount.orElse(if (s.getPassengerCount() == null) None else Option(s.getPassengerCount()))
            val tempTripDistance: Option[Double] = tripDistance.orElse(if (s.getTripDistance() == null) None else Option(s.getTripDistance()))
            val tempFareAmount: Option[Double] = fareAmount.orElse(if (s.getFareAmount() == null) None else Option(s.getFareAmount()))
            val tempExtra: Option[Double] = extra.orElse(if (s.getExtra() == null) None else Option(s.getExtra()))
            val tempMtaTax: Option[Double] = mtaTax.orElse(if (s.getMtaTax() == null) None else Option(s.getMtaTax()))
            val tempTipAmount: Option[Double] = tipAmount.orElse(if (s.getTipAmount() == null) None else Option(s.getTipAmount()))
            val tempTollsAmount: Option[Double] = tollsAmount.orElse(if (s.getTollsAmount() == null) None else Option(s.getTollsAmount()))
            val tempEhailFee: Option[Double] = ehailFee.orElse(if (s.getEhailFee() == null) None else Option(s.getEhailFee()))
            val tempImprovementSurcharge: Option[Double] = improvementSurcharge.orElse(if (s.getImprovementSurcharge() == null) None else Option(s.getImprovementSurcharge()))
            val tempTotalAmount: Option[Double] = totalAmount.orElse(if (s.getTotalAmount() == null) None else Option(s.getTotalAmount()))
            val tempPaymentType: Option[Double] = paymentType.orElse(if (s.getPaymentType() == null) None else Option(s.getPaymentType()))
            val tempTripType: Option[Double] = tripType.orElse(if (s.getTripType() == null) None else Option(s.getTripType()))
            tempVendorId.foreach(t.setVendorId(_))
            tempLpepPickUpDatetime.foreach(t.setLpepPickUpDatetime(_))
            tempLpepDropOffDatetime.foreach(t.setLpepDropOffDatetime(_))
            tempStoreAndFwdFlag.foreach(t.setStoreAndFwdFlag(_))
            tempRatecodeID.foreach(t.setRatecodeID(_))
            tempPickUpLocationId.foreach(t.setPickUpLocationId(_))
            tempDropOffLocationId.foreach(t.setDropOffLocationId(_))
            tempPassengerCount.foreach(t.setPassengerCount(_))
            tempTripDistance.foreach(t.setTripDistance(_))
            tempFareAmount.foreach(t.setFareAmount(_))
            tempExtra.foreach(t.setExtra(_))
            tempMtaTax.foreach(t.setMtaTax(_))
            tempTipAmount.foreach(t.setTipAmount(_))
            tempTollsAmount.foreach(t.setTollsAmount(_))
            tempEhailFee.foreach(t.setEhailFee(_))
            tempImprovementSurcharge.foreach(t.setImprovementSurcharge(_))
            tempTotalAmount.foreach(t.setTotalAmount(_))
            tempPaymentType.foreach(t.setPaymentType(_))
            tempTripType.foreach(t.setTripType(_))

            t
        }
    }
}

