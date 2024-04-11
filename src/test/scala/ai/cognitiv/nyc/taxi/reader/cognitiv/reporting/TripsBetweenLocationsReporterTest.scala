package ai.cognitiv.nyc.taxi.reader.cognitiv.reporting

import ai.cognitiv.nyc.taxi.reader.cognitiv.ImplicitHelpers._
import ai.cognitiv.nyc.taxi.reader.cognitiv.LocalSparkBase
import ai.cognitiv.nyc.taxi.reader.cognitiv.model.{AggregatedTripData, AnalyzedTripRecord, TaxiColor}
import org.apache.spark.sql
import org.apache.spark.sql.{Dataset, Encoders}

import java.sql.Timestamp
import java.time.LocalDateTime

class TripsBetweenLocationsReporterTest extends LocalSparkBase with Reportings {

    val start: Timestamp = Timestamp.valueOf(LocalDateTime.parse("2024-02-15T19:56:35"))
    val end: Timestamp = Timestamp.valueOf(LocalDateTime.parse("2024-02-16T19:56:35"))

    Feature("Should create aggregated report for trip between 2 locations") {

        implicit val encoder: sql.Encoder[AnalyzedTripRecord] = Encoders.bean(classOf[AnalyzedTripRecord])
        implicit val encoder2: sql.Encoder[AggregatedTripData] = Encoders.bean(classOf[AggregatedTripData])
        Scenario("One trip is in pick up and drop off") {
            Given("subject and session")
            import spark.implicits._

            val startLocation = 1
            val endLocation = 2

            val subject: TripsBetweenLocationsReporter = new TripsBetweenLocationsReporter(startLocation, endLocation)
            val ds = Seq(
                toAnalyzedTripRecord(vendorId = 1, start, end, fareAmount = 1, tollAmount = 1, paymentType = 2, startLocation.toLong.toOpt, endLocation.toLong.toOpt)
            ).toDS()

            And("Expected dataset")
            val expected = Seq(
                toAggregatedTripData(paymentType = 2, minFare = 1, maxFare = 1, count = 1, fareSum = 1, tollsSum = 1)
            ).toDS()

            When("Report is generated")
            val actual = subject.generate(spark, ds)
            Then("actual matches expected")
            assertDatasetEquals(expected, actual)
        }

        Scenario("does not return result when only pick up location matches") {
            Given("subject and session")
            import spark.implicits._

            val startLocation = 1
            val endLocation = 4

            val nonMatchingDropOffLocation = 2

            val subject: TripsBetweenLocationsReporter = new TripsBetweenLocationsReporter(startLocation, endLocation)
            val inputDataset = Seq(
                toAnalyzedTripRecord(vendorId = 1, start, end, fareAmount = 1, tollAmount = 1, paymentType = 2, startLocation.toLong.toOpt, nonMatchingDropOffLocation.toLong.toOpt)
            ).toDS()

            And("Expected dataset")
            val expected = spark.emptyDataset(encoder2)

            When("Report is generated")
            val actual = subject.generate(spark, inputDataset)
            Then("actual matches expected")
            assertDatasetEquals(expected, actual)
        }

        Scenario("does not return result when only drop off location matches") {
            Given("subject and session")
            import spark.implicits._

            val startLocation = 1
            val endLocation = 4

            val nonMatchingPickUpLocation = 2

            val subject: TripsBetweenLocationsReporter = new TripsBetweenLocationsReporter(startLocation, endLocation)
            val inputDataset = Seq(
                toAnalyzedTripRecord(vendorId = 1, start, end, fareAmount = 1, tollAmount = 1, paymentType = 2, nonMatchingPickUpLocation.toLong.toOpt, endLocation.toLong.toOpt)
            ).toDS()

            And("Expected dataset")
            val expected = spark.emptyDataset(encoder2)

            When("Report is generated")
            val actual = subject.generate(spark, inputDataset)
            Then("actual matches expected")
            assertDatasetEquals(expected, actual)
        }

        Scenario("Groups multiple trips when pick up and drop off location match for same payment type") {
            Given("subject and session")
            import spark.implicits._

            val startLocation = 1
            val endLocation = 2

            val subject: TripsBetweenLocationsReporter = new TripsBetweenLocationsReporter(startLocation, endLocation)
            val ds = Seq(
                toAnalyzedTripRecord(vendorId = 1, start, end, fareAmount = 1, tollAmount = 1, paymentType = 2, startLocation.toLong.toOpt, endLocation.toLong.toOpt),
                toAnalyzedTripRecord(vendorId = 1, start, end, fareAmount = 2, tollAmount = 3, paymentType = 2, startLocation.toLong.toOpt, endLocation.toLong.toOpt)
            ).toDS()

            And("Expected dataset")
            val expected = Seq(
                toAggregatedTripData(paymentType = 2, minFare = 1, maxFare = 2, count = 2, fareSum = 3, tollsSum = 4)
            ).toDS()

            When("Report is generated")
            val actual = subject.generate(spark, ds)
            Then("actual matches expected")
            assertDatasetEquals(expected, actual)
        }

        Scenario("Groups multiple trips when pick up and drop off location match for different payment type") {
            Given("subject and session")
            import spark.implicits._

            val startLocation = 1
            val endLocation = 2

            val subject: TripsBetweenLocationsReporter = new TripsBetweenLocationsReporter(startLocation, endLocation)
            val ds = Seq(
                toAnalyzedTripRecord(vendorId = 1, start, end, fareAmount = 1, tollAmount = 1, paymentType = 2, startLocation.toLong.toOpt, endLocation.toLong.toOpt),
                toAnalyzedTripRecord(vendorId = 1, start, end, fareAmount = 2, tollAmount = 3, paymentType = 1, startLocation.toLong.toOpt, endLocation.toLong.toOpt)
            ).toDS()

            And("Expected dataset")
            val expected = Seq(
                toAggregatedTripData(paymentType = 2, minFare = 1, maxFare = 1, count = 1, fareSum = 1, tollsSum = 1),
                toAggregatedTripData(paymentType = 1, minFare = 2, maxFare = 2, count = 1, fareSum = 2, tollsSum = 3)
            ).toDS()

            When("Report is generated")
            val actual = subject.generate(spark, ds)
            Then("actual matches expected")
            assertDatasetEquals(expected, actual)
        }

        Scenario("Can search on empty Dataset") {
            Given("locations to search for")

            And("subject")
            val subject = new TripsBetweenLocationsReporter(1, 2);

            And("input dataset")
            val inputDs: Dataset[AnalyzedTripRecord] = spark.emptyDataset

            And("Expected dataset")
            val expected: Dataset[AggregatedTripData] = spark.emptyDataset

            When("Report is generated")
            val actual = subject.generate(spark, inputDs)
            Then("Empty dataset returned")
            assertDatasetEquals(expected, actual)
        }
    }

}
