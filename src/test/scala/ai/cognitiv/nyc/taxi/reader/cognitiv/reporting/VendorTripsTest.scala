package ai.cognitiv.nyc.taxi.reader.cognitiv.reporting

import ai.cognitiv.nyc.taxi.reader.cognitiv.LocalSparkBase
import ai.cognitiv.nyc.taxi.reader.cognitiv.model.{AggregatedTripData, AnalyzedTripRecord}
import org.apache.spark.sql
import org.apache.spark.sql.{Dataset, Encoders}

import java.sql.Timestamp
import java.time.LocalDateTime

class VendorTripsTest extends LocalSparkBase with Reportings {

    val start: Timestamp = Timestamp.valueOf(LocalDateTime.parse("2024-02-15T19:56:35"))
    val end: Timestamp = Timestamp.valueOf(LocalDateTime.parse("2024-02-16T19:56:35"))

    Feature("Vendor specific report") {
        import spark.implicits._

        implicit val encoder: sql.Encoder[AnalyzedTripRecord] = Encoders.bean(classOf[AnalyzedTripRecord])
        implicit val encoder2: sql.Encoder[AggregatedTripData] = Encoders.bean(classOf[AggregatedTripData])

        Scenario("Select all the trips for searched vendor") {
            Given("Vendor to search for")
            val searchedVendorId = 1
            val otherVendorId = 2

            And("subject")
            val subject = new VendorTrips(searchedVendorId);

            And("input dataset")
            val inputDs: Dataset[AnalyzedTripRecord] = Seq(
                toAnalyzedTripRecord(vendorId = searchedVendorId, start, end, fareAmount = 1, tollAmount = 1, paymentType = 2),
                toAnalyzedTripRecord(vendorId = otherVendorId, start, end, fareAmount = 2, tollAmount = 3, paymentType = 2)
            ).toDS()

            And("Expected dataset")
            val expected = Seq(
                toAggregatedTripData(paymentType = 2, minFare = 1, maxFare = 1, count = 1, fareSum = 1, tollsSum = 1)
            ).toDS()

            When("Report is generated")
            val actual = subject.generate(spark, inputDs)
            Then("Only searched vendor returned")
            assertDatasetEquals(expected, actual)
        }

        Scenario("Select all the trips for searched vendor with aggregated data") {
            Given("Vendor to search for")
            val searchedVendorId = 1
            val otherVendorId = 2

            And("subject")
            val subject = new VendorTrips(searchedVendorId);

            And("input dataset")
            val inputDs: Dataset[AnalyzedTripRecord] = Seq(
                toAnalyzedTripRecord(vendorId = searchedVendorId, start, end, fareAmount = 1, tollAmount = 1, paymentType = 2),
                toAnalyzedTripRecord(vendorId = searchedVendorId, start, end, fareAmount = 5, tollAmount = 6, paymentType = 2),
                toAnalyzedTripRecord(vendorId = otherVendorId, start, end, fareAmount = 2, tollAmount = 3, paymentType = 2)
            ).toDS()

            And("Expected dataset")
            val expected = Seq(
                toAggregatedTripData(paymentType = 2, minFare = 1, maxFare = 5, count = 2, fareSum = 6, tollsSum = 7)
            ).toDS()

            When("Report is generated")
            val actual = subject.generate(spark, inputDs)
            Then("Only searched vendor returned")
            assertDatasetEquals(expected, actual)
        }

        Scenario("Select all the trips for searched vendor with grouped by payment type") {
            Given("Vendor to search for")
            val searchedVendorId = 1
            val otherVendorId = 2

            And("subject")
            val subject = new VendorTrips(searchedVendorId);

            And("input dataset")
            val inputDs: Dataset[AnalyzedTripRecord] = Seq(
                toAnalyzedTripRecord(vendorId = searchedVendorId, start, end, fareAmount = 1, tollAmount = 1, paymentType = 2),
                toAnalyzedTripRecord(vendorId = searchedVendorId, start, end, fareAmount = 5, tollAmount = 6, paymentType = 1),
                toAnalyzedTripRecord(vendorId = otherVendorId, start, end, fareAmount = 2, tollAmount = 3, paymentType = 2)
            ).toDS()

            And("Expected dataset")
            val expected = Seq(
                toAggregatedTripData(paymentType = 2, minFare = 1, maxFare = 1, count = 1, fareSum = 1, tollsSum = 1),
                toAggregatedTripData(paymentType = 1, minFare = 5, maxFare = 5, count = 1, fareSum = 5, tollsSum = 6)
            ).toDS()

            When("Report is generated")
            val actual = subject.generate(spark, inputDs)
            Then("Only searched vendor returned")
            assertDatasetEquals(expected, actual)
        }

        Scenario("Can search on empty Dataset") {
            Given("time interval to search for")

            And("subject")
            val subject = new VendorTrips(5);

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
