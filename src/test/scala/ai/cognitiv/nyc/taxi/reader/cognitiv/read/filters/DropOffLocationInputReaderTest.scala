package ai.cognitiv.nyc.taxi.reader.cognitiv.read.filters

import ai.cognitiv.nyc.taxi.reader.cognitiv.ImplicitHelpers._
import ai.cognitiv.nyc.taxi.reader.cognitiv.LocalSparkBase
import ai.cognitiv.nyc.taxi.reader.cognitiv.model.{GreenTripRecord, YellowTripRecord}
import ai.cognitiv.nyc.taxi.reader.cognitiv.read.{Readings, TaxiReader}
import org.apache.spark.sql.{Dataset, Encoders}
import org.mockito.Mockito

class DropOffLocationInputReaderTest extends LocalSparkBase with Readings {

    Feature("Drops all entries outside of DropOff location") {
        import spark.implicits._

        implicit val enc1 = Encoders.bean(classOf[YellowTripRecord])
        implicit val enc2 = Encoders.bean(classOf[GreenTripRecord])

        Scenario("Drops all yellow records outside of the interval") {
            Given("Input parameters")
            val targetDropOffLocation = yellowSample().getDropOffLocationId

            And("Mock setup")
            val inputReader = mock[TaxiReader[YellowTripRecord]](Mockito.withSettings().serializable())
            val ds: Dataset[YellowTripRecord] = Seq(
                yellowSample(),
                yellowSample().copy(dropOffLocationId = 10L.toOpt),
                yellowSample().copy(dropOffLocationId = 20L.toOpt),
                yellowSample().copy(dropOffLocationId = 30L.toOpt),
                yellowSample(),
            ).toDS()
            Mockito.when(inputReader.readInput(spark)).thenReturn(ds)

            And("subject")
            val subject = new DropOffLocationInputReader(YellowTripRecord.EntityNames.DOLOCATIONID, targetDropOffLocation, inputReader, classOf[YellowTripRecord])

            When("Input is read")
            val actual = subject.readInput(spark)

            Then(s"Only dropOffLocationId=${targetDropOffLocation} should remain in dataset")
            val set = actual.collect().map(_.getDropOffLocationId).toSet
            set should contain only targetDropOffLocation
        }

        Scenario("Drops all green records outside of the interval") {
            Given("Input parameters")
            val targetDropOffLocation = greenSample().getDropOffLocationId

            And("Mock setup")
            val inputReader = mock[TaxiReader[GreenTripRecord]](Mockito.withSettings().serializable())
            val ds: Dataset[GreenTripRecord] = Seq(
                greenSample(),
                greenSample().copy(dropOffLocationId = 10L.toOpt),
                greenSample().copy(dropOffLocationId = 20L.toOpt),
                greenSample().copy(dropOffLocationId = 30L.toOpt),
                greenSample(),
            ).toDS()
            Mockito.when(inputReader.readInput(spark)).thenReturn(ds)

            And("subject")
            val subject = new DropOffLocationInputReader(GreenTripRecord.EntityNames.DO_LOCATION_ID, targetDropOffLocation, inputReader, classOf[GreenTripRecord])

            When("Input is read")
            val actual = subject.readInput(spark)

            Then(s"Only dropOffLocationId=${targetDropOffLocation} should remain in dataset")
            val set = actual.collect().map(_.getDropOffLocationId).toSet
            set should contain only targetDropOffLocation
        }

    }

}
