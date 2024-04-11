package ai.cognitiv.nyc.taxi.reader.cognitiv.reporting

import ai.cognitiv.nyc.taxi.reader.cognitiv.ImplicitHelpers._
import ai.cognitiv.nyc.taxi.reader.cognitiv.LocalSparkBase
import ai.cognitiv.nyc.taxi.reader.cognitiv.model.{AggregatedTripData, AnalyzedTripRecord, TaxiColor}
import org.apache.spark.sql
import org.apache.spark.sql.{Dataset, Encoders}

import java.sql.Timestamp
import java.time.LocalDateTime

class TaxiColorReportTest extends LocalSparkBase with Reportings {

    val start: Timestamp = Timestamp.valueOf(LocalDateTime.parse("2024-02-15T19:56:35"))
    val end: Timestamp = Timestamp.valueOf(LocalDateTime.parse("2024-02-16T19:56:35"))

    Feature("Creates report on a specific taxi color") {

        import spark.implicits._

        implicit val encoder: sql.Encoder[AnalyzedTripRecord] = Encoders.bean(classOf[AnalyzedTripRecord])
        implicit val encoder2: sql.Encoder[AggregatedTripData] = Encoders.bean(classOf[AggregatedTripData])

        Scenario("Aggregates on green taxis only") {
            Given("Color to search for")

            And("subject")
            val subject = new TaxiColorReport(TaxiColor.GREEN);

            And("input dataset")
            val inputDs: Dataset[AnalyzedTripRecord] = Seq(
                toAnalyzedTripRecord(vendorId = 1, start, end, fareAmount = 1, tollAmount = 1, paymentType = 2, color = TaxiColor.YELLOW.toOpt),
                toAnalyzedTripRecord(vendorId = 1, start, end, fareAmount = 2, tollAmount = 3, paymentType = 2, color = TaxiColor.GREEN.toOpt),
                toAnalyzedTripRecord(vendorId = 2, start, end, fareAmount = 5, tollAmount = 8, paymentType = 2, color = TaxiColor.GREEN.toOpt)
            ).toDS()

            And("Expected dataset")
            val expected = Seq(
                toAggregatedTripData(paymentType = 2, minFare = 2, maxFare = 5, count = 2, fareSum = 7, tollsSum = 11)
            ).toDS()

            When("Report is generated")
            val actual = subject.generate(spark, inputDs)
            Then("Only searched color returned")
            assertDatasetEquals(expected, actual)
        }

        Scenario("Aggregates on green taxis with different payment types") {
            Given("Color to search for")

            And("subject")
            val subject = new TaxiColorReport(TaxiColor.GREEN);

            And("input dataset")
            val inputDs: Dataset[AnalyzedTripRecord] = Seq(
                toAnalyzedTripRecord(vendorId = 1, start, end, fareAmount = 1, tollAmount = 1, paymentType = 2, color = TaxiColor.YELLOW.toOpt),
                toAnalyzedTripRecord(vendorId = 1, start, end, fareAmount = 2, tollAmount = 3, paymentType = 1, color = TaxiColor.GREEN.toOpt),
                toAnalyzedTripRecord(vendorId = 2, start, end, fareAmount = 5, tollAmount = 8, paymentType = 2, color = TaxiColor.GREEN.toOpt)
            ).toDS()

            And("Expected dataset")
            val expected = Seq(
                toAggregatedTripData(paymentType = 1, minFare = 2, maxFare = 2, count = 1, fareSum = 2, tollsSum = 3),
                toAggregatedTripData(paymentType = 2, minFare = 5, maxFare = 5, count = 1, fareSum = 5, tollsSum = 8)
            ).toDS()

            When("Report is generated")
            val actual = subject.generate(spark, inputDs)
            Then("Only searched color returned")
            assertDatasetEquals(expected, actual)
        }

        Scenario("Can search on empty Dataset") {
            Given("Color to search for")

            And("subject")
            val subject = new TaxiColorReport(TaxiColor.GREEN);

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
