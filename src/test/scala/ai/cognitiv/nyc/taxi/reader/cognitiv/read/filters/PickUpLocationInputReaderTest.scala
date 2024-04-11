package ai.cognitiv.nyc.taxi.reader.cognitiv.read.filters

import ai.cognitiv.nyc.taxi.reader.cognitiv.LocalSparkBase
import ai.cognitiv.nyc.taxi.reader.cognitiv.model.{GreenTripRecord, YellowTripRecord}
import ai.cognitiv.nyc.taxi.reader.cognitiv.read.{Readings, TaxiReader}
import org.apache.spark.sql.{Dataset, Encoders}
import org.mockito.Mockito

import ai.cognitiv.nyc.taxi.reader.cognitiv.ImplicitHelpers._

class PickUpLocationInputReaderTest extends LocalSparkBase with Readings {

    Feature("Drops all entries outside of pickup location") {
        import spark.implicits._

        implicit val enc1 = Encoders.bean(classOf[YellowTripRecord])
        implicit val enc2 = Encoders.bean(classOf[GreenTripRecord])

        Scenario("Drops all yellow records outside of the interval") {
            Given("Input parameters")
            val targetPickUpLocation = yellowSample().getPickUpLocationId

            And("Mock setup")
            val inputReader = mock[TaxiReader[YellowTripRecord]](Mockito.withSettings().serializable())
            val ds: Dataset[YellowTripRecord] = Seq(
                yellowSample(),
                yellowSample().copy(pickUpLocationId = 10L.toOpt),
                yellowSample().copy(pickUpLocationId = 20L.toOpt),
                yellowSample().copy(pickUpLocationId = 30L.toOpt),
                yellowSample(),
            ).toDS()
            Mockito.when(inputReader.readInput(spark)).thenReturn(ds)

            And("subject")
            val subject = new PickUpLocationInputReader(YellowTripRecord.EntityNames.PULOCATIONID, targetPickUpLocation, inputReader, classOf[YellowTripRecord])

            When("Input is read")
            val actual = subject.readInput(spark)

            Then(s"Only pickupLocationId=${targetPickUpLocation} should remain in dataset")
            val set = actual.collect().map(_.getPickUpLocationId).toSet
            set should contain only targetPickUpLocation
        }

        Scenario("Drops all green records outside of the interval") {
            Given("Input parameters")
            val targetPickUpLocation = greenSample().getPickUpLocationId

            And("Mock setup")
            val inputReader = mock[TaxiReader[GreenTripRecord]](Mockito.withSettings().serializable())
            val ds: Dataset[GreenTripRecord] = Seq(
                greenSample(),
                greenSample().copy(pickUpLocationId = 10L.toOpt),
                greenSample().copy(pickUpLocationId = 20L.toOpt),
                greenSample().copy(pickUpLocationId = 30L.toOpt),
                greenSample(),
            ).toDS()
            Mockito.when(inputReader.readInput(spark)).thenReturn(ds)

            And("subject")
            val subject = new PickUpLocationInputReader(GreenTripRecord.EntityNames.PU_LOCATION_ID, targetPickUpLocation, inputReader, classOf[GreenTripRecord])

            When("Input is read")
            val actual = subject.readInput(spark)

            Then(s"Only pickupLocationId=${targetPickUpLocation} should remain in dataset")
            val set = actual.collect().map(_.getPickUpLocationId).toSet
            set should contain only targetPickUpLocation
        }

    }

}
