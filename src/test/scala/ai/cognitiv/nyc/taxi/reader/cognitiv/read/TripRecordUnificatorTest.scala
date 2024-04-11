package ai.cognitiv.nyc.taxi.reader.cognitiv.read

import ai.cognitiv.nyc.taxi.reader.cognitiv.LocalSparkBase
import ai.cognitiv.nyc.taxi.reader.cognitiv.model.{AnalyzedTripRecord, GreenTripRecord, YellowTripRecord}
import org.apache.spark.sql.{Dataset, Encoders}
import org.mockito.Mockito

class TripRecordUnificatorTest extends LocalSparkBase with Readings {

    Feature("Should unite 2 datasets") {
        import spark.implicits._
        implicit val enc1 = Encoders.bean(classOf[YellowTripRecord])
        implicit val enc2 = Encoders.bean(classOf[GreenTripRecord])
        Scenario("Unites green and yellow taxi") {
            Given("Mocks")
            val greenReader: TaxiReader[GreenTripRecord] = mock[TaxiReader[GreenTripRecord]](Mockito.withSettings().serializable())
            val yellowReader = mock[TaxiReader[YellowTripRecord]](Mockito.withSettings().serializable())

            val yellowSet: Dataset[YellowTripRecord] = Seq(
                yellowSample()
            ).toDS()

            val greenSet: Dataset[GreenTripRecord] = Seq(
                greenSample()
            ).toDS()

            Mockito.when(greenReader.readInput(spark)).thenReturn(greenSet)
            Mockito.when(yellowReader.readInput(spark)).thenReturn(yellowSet)

            And("subject")
            val subject: TaxiReader[AnalyzedTripRecord] = new TripRecordUnificator(yellowReader, greenReader)

            When("United")
            val actual = subject.readInput(spark)

            Then("Should match expectation")
            actual.count() shouldBe 2
        }

        Scenario("Unites green and empty yellow taxi") {
            Given("Mocks")
            val greenReader: TaxiReader[GreenTripRecord] = mock[TaxiReader[GreenTripRecord]](Mockito.withSettings().serializable())
            val yellowReader = mock[TaxiReader[YellowTripRecord]](Mockito.withSettings().serializable())

            val yellowSet: Dataset[YellowTripRecord] = spark.emptyDataset

            val greenSet: Dataset[GreenTripRecord] = Seq(
                greenSample()
            ).toDS()

            Mockito.when(greenReader.readInput(spark)).thenReturn(greenSet)
            Mockito.when(yellowReader.readInput(spark)).thenReturn(yellowSet)

            And("subject")
            val subject: TaxiReader[AnalyzedTripRecord] = new TripRecordUnificator(yellowReader, greenReader)

            When("United")
            val actual = subject.readInput(spark)

            Then("Should match expectation")
            actual.count() shouldBe 1
        }

        Scenario("Unites empty green and yellow taxi") {
            Given("Mocks")
            val greenReader: TaxiReader[GreenTripRecord] = mock[TaxiReader[GreenTripRecord]](Mockito.withSettings().serializable())
            val yellowReader = mock[TaxiReader[YellowTripRecord]](Mockito.withSettings().serializable())

            val yellowSet = Seq(
                yellowSample()
            ).toDS()

            val greenSet: Dataset[GreenTripRecord] = spark.emptyDataset

            Mockito.when(greenReader.readInput(spark)).thenReturn(greenSet)
            Mockito.when(yellowReader.readInput(spark)).thenReturn(yellowSet)

            And("subject")
            val subject: TaxiReader[AnalyzedTripRecord] = new TripRecordUnificator(yellowReader, greenReader)

            When("United")
            val actual = subject.readInput(spark)

            Then("Should match expectation")
            actual.count() shouldBe 1
        }
    }

}
