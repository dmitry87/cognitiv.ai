package ai.cognitiv.nyc.taxi.reader.cognitiv.read.filters

import ai.cognitiv.nyc.taxi.reader.cognitiv.ImplicitHelpers._
import ai.cognitiv.nyc.taxi.reader.cognitiv.LocalSparkBase
import ai.cognitiv.nyc.taxi.reader.cognitiv.model.{GreenTripRecord, YellowTripRecord}
import ai.cognitiv.nyc.taxi.reader.cognitiv.read.{Readings, TaxiReader}
import org.apache.spark.sql.{Dataset, Encoders}
import org.mockito.Mockito

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class TimeBeforeInputFilterTest extends LocalSparkBase with Readings {

    Feature("Drop all entries after specified time") {
        import spark.implicits._

        implicit val enc1 = Encoders.bean(classOf[YellowTripRecord])
        implicit val enc2 = Encoders.bean(classOf[GreenTripRecord])

        Scenario("All records before provided time interval should remain for Yellow taxis") {
            Given("initial settings")

            val searchedTimeColumnName = YellowTripRecord.EntityNames.TPEP_PICKUP_DATETIME
            val dropAllRecordsAfter = LocalDateTime.parse("2023-10-30T17:16:44.00")
            val startDate = yellowSample().getTpepPickUpDatetime.toLocalDateTime
            val endDate = yellowSample().getTpepDropOffDatetime.toLocalDateTime

            And("Mock setup")
            val yellowTaxiReader = mock[TaxiReader[YellowTripRecord]](Mockito.withSettings().serializable())
            val inputDs: Dataset[YellowTripRecord] = Seq(
                yellowSample(),
                yellowSample().copy(tpepPickUpDatetime = startDate.plusMonths(1).format(DateTimeFormatter.ISO_DATE_TIME).toOpt, tpepDropOffDatetime = endDate.plusMonths(1).format(DateTimeFormatter.ISO_DATE_TIME).toOpt),
                yellowSample().copy(tpepPickUpDatetime = startDate.plusMonths(4).format(DateTimeFormatter.ISO_DATE_TIME).toOpt, tpepDropOffDatetime = endDate.plusMonths(4).format(DateTimeFormatter.ISO_DATE_TIME).toOpt),
                yellowSample().copy(tpepPickUpDatetime = startDate.plusMonths(12).format(DateTimeFormatter.ISO_DATE_TIME).toOpt, tpepDropOffDatetime = endDate.plusMonths(12).format(DateTimeFormatter.ISO_DATE_TIME).toOpt),

            ).toDS()
            Mockito.when(yellowTaxiReader.readInput(spark)).thenReturn(inputDs)

            And("Subject")
            val subject = new TimeBeforeInputReader(searchedTimeColumnName, dropAllRecordsAfter, yellowTaxiReader, classOf[YellowTripRecord])

            When("input is filtered")
            val actual = subject.readInput(spark)
            Then(s"All records older ${dropAllRecordsAfter} are dropped")
            actual.collect()
                .map(_.getTpepPickUpDatetime.toLocalDateTime)
                .foreach(pickUpDateTime =>
                    pickUpDateTime should be < dropAllRecordsAfter)
        }

        Scenario("All records before provided time interval should remain for Green taxis") {
            Given("initial settings")

            val searchedTimeColumnName = GreenTripRecord.EntityNames.LPEP_PICKUP_DATETIME
            val dropAllRecordsAfter = LocalDateTime.parse("2023-10-30T17:16:44.00")
            val startDate = greenSample().getLpepPickUpDatetime.toLocalDateTime
            val endDate = greenSample().getLpepDropOffDatetime.toLocalDateTime

            And("Mock setup")
            val greenTaxiReader = mock[TaxiReader[GreenTripRecord]](Mockito.withSettings().serializable())
            val inputDs: Dataset[GreenTripRecord] = Seq(
                greenSample(),
                greenSample().copy(lpepPickUpDatetime = startDate.plusMonths(1).format(DateTimeFormatter.ISO_DATE_TIME).toOpt, lpepDropOffDatetime = endDate.plusMonths(1).format(DateTimeFormatter.ISO_DATE_TIME).toOpt),
                greenSample().copy(lpepPickUpDatetime = startDate.plusMonths(4).format(DateTimeFormatter.ISO_DATE_TIME).toOpt, lpepDropOffDatetime = endDate.plusMonths(4).format(DateTimeFormatter.ISO_DATE_TIME).toOpt),
                greenSample().copy(lpepPickUpDatetime = startDate.plusMonths(12).format(DateTimeFormatter.ISO_DATE_TIME).toOpt, lpepDropOffDatetime = endDate.plusMonths(12).format(DateTimeFormatter.ISO_DATE_TIME).toOpt),

            ).toDS()
            Mockito.when(greenTaxiReader.readInput(spark)).thenReturn(inputDs)

            And("Subject")
            val subject = new TimeBeforeInputReader(searchedTimeColumnName, dropAllRecordsAfter, greenTaxiReader, classOf[GreenTripRecord])

            When("input is filtered")
            val actual = subject.readInput(spark)
            Then(s"All records older ${dropAllRecordsAfter} are dropped")
            actual.collect()
                .map(_.getLpepPickUpDatetime.toLocalDateTime)
                .foreach(pickUpDateTime =>
                    pickUpDateTime should be < dropAllRecordsAfter)
        }
    }

}
