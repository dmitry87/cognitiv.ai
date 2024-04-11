package ai.cognitiv.nyc.taxi.reader.cognitiv.reporting

import ai.cognitiv.nyc.taxi.reader.cognitiv.LocalSparkBase
import ai.cognitiv.nyc.taxi.reader.cognitiv.model.{AggregatedTripData, AnalyzedTripRecord}
import org.apache.spark.sql
import org.apache.spark.sql.{Dataset, Encoders}

import java.sql.Timestamp
import java.time.LocalDateTime

class TripsBetweenTimesReporterTest extends LocalSparkBase with Reportings {

    val start: LocalDateTime = LocalDateTime.parse("2024-02-15T19:56:35");
    val end: LocalDateTime = LocalDateTime.parse("2024-02-16T19:56:35");

    Feature("Runs queries within time intervals") {
        import spark.implicits._

        implicit val encoder: sql.Encoder[AnalyzedTripRecord] = Encoders.bean(classOf[AnalyzedTripRecord])
        implicit val encoder2: sql.Encoder[AggregatedTripData] = Encoders.bean(classOf[AggregatedTripData])

        Scenario("Happy path interval test") {
            Given("subject and session")

            val subject: TripsBetweenTimesReporter = new TripsBetweenTimesReporter(start, end)
            val ds = Seq(
                toAnalyzedTripRecord(1, Timestamp.valueOf(start.plusHours(1)), Timestamp.valueOf(start.plusHours(2)), 1, 1, 2)
            ).toDS()

            And("Expected DS")
            val expected: Dataset[AggregatedTripData] = Seq(
                toAggregatedTripData(2.0, 1.0, 1.0, 1, 1.0, 1.0)
            ).toDS()

            When("Aggregated")
            val agg: Dataset[AggregatedTripData] = subject.generate(spark, ds)
            Then("Should match expectation")
            val result = agg.collect().toList
            result should contain theSameElementsAs expected.collect().toList

        }

        Scenario("Aggregates 2 entries") {
            Given("subject and session")

            val subject: TripsBetweenTimesReporter = new TripsBetweenTimesReporter(start, end)
            val ds = Seq(
                toAnalyzedTripRecord(1, Timestamp.valueOf(start.plusHours(1)), Timestamp.valueOf(start.plusHours(2)), 1D, 1D, 2D),
                toAnalyzedTripRecord(1, Timestamp.valueOf(start.plusHours(1)), Timestamp.valueOf(start.plusHours(2)), 11D, 12D, 2D)
            ).toDS()

            And("Expected DS")
            val expected: Dataset[AggregatedTripData] = Seq(
                toAggregatedTripData(2.0, 1.0, 11.0, 2, 12.0, 13.0)
            ).toDS()

            When("Aggregated")
            val agg = subject.generate(spark, ds)
            Then("Should match expectation")
            assertDatasetEquals(expected, agg)
        }

        Scenario("One Pick up happened earlier") {
            Given("subject and session")

            val subject: TripsBetweenTimesReporter = new TripsBetweenTimesReporter(start, end)
            And("Earlier trip")
            val earlierTrip = toAnalyzedTripRecord(1, Timestamp.valueOf(start.minusHours(1)), Timestamp.valueOf(start.plusHours(2)), 1D, 1D, 2D)

            val ds = Seq(
                earlierTrip,
                toAnalyzedTripRecord(1, Timestamp.valueOf(start.plusHours(1)), Timestamp.valueOf(start.plusHours(2)), 11D, 12D, 2D)
            ).toDS()

            And("Expected DS")
            val expected: Dataset[AggregatedTripData] = Seq(
                toAggregatedTripData(2.0, 11.0, 11.0, 1, 11.0, 12.0)
            ).toDS()

            When("Aggregated")
            val agg = subject.generate(spark, ds)
            Then("Should match expectation")
            assertDatasetEquals(expected, agg)
        }

        Scenario("One Pick up happened Later") {
            Given("subject and session")

            val subject: TripsBetweenTimesReporter = new TripsBetweenTimesReporter(start, end)
            And("Earlier trip")
            val laterTrip = toAnalyzedTripRecord(1, Timestamp.valueOf(end.plusHours(1)), Timestamp.valueOf(end.plusHours(2)), 1D, 1D, 2D)

            val ds = Seq(
                laterTrip,
                toAnalyzedTripRecord(1, Timestamp.valueOf(start.plusHours(1)), Timestamp.valueOf(start.plusHours(2)), 11D, 12D, 2D)
            ).toDS()

            And("Expected DS")
            val expected: Dataset[AggregatedTripData] = Seq(
                toAggregatedTripData(2.0, 11.0, 11.0, 1, 11.0, 12.0)
            ).toDS()

            When("Aggregated")
            val agg = subject.generate(spark, ds)
            Then("Should match expectation")
            assertDatasetEquals(expected, agg)
        }

        Scenario("2 trips with different payment types") {
            Given("subject and session")

            val subject: TripsBetweenTimesReporter = new TripsBetweenTimesReporter(start, end)
            And("Other payment type trip trip")
            val ccTrip = toAnalyzedTripRecord(1, Timestamp.valueOf(start.plusHours(1)), Timestamp.valueOf(start.plusHours(2)), 1D, 1D, 1D)
            val cashTrip = toAnalyzedTripRecord(1, Timestamp.valueOf(start.plusHours(1)), Timestamp.valueOf(start.plusHours(2)), 11D, 12D, 2D)

            val ds = Seq(
                ccTrip,
                cashTrip
            ).toDS()

            And("Expected DS")
            val expected: Dataset[AggregatedTripData] = Seq(
                toAggregatedTripData(2.0, 11.0, 11.0, 1, 11.0, 12.0),
                toAggregatedTripData(1.0, 1.0, 1.0, 1, 1.0, 1.0)
            ).toDS()

            When("Aggregated")
            val agg = subject.generate(spark, ds)
            Then("Should match expectation")
            agg.collect() should contain theSameElementsAs (expected.collect())
        }

        Scenario("Can search on empty Dataset") {
            Given("time interval to search for")

            And("subject")
            val subject = new TripsBetweenTimesReporter(start, end);

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
