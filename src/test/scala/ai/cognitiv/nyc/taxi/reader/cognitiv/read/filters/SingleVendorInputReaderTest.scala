package ai.cognitiv.nyc.taxi.reader.cognitiv.read.filters

import ai.cognitiv.nyc.taxi.reader.cognitiv.ImplicitHelpers._
import ai.cognitiv.nyc.taxi.reader.cognitiv.LocalSparkBase
import ai.cognitiv.nyc.taxi.reader.cognitiv.model.{GreenTripRecord, YellowTripRecord}
import ai.cognitiv.nyc.taxi.reader.cognitiv.read.{GenericTaxiReader, Readings}
import org.apache.spark.sql.Encoders
import org.mockito.Mockito

class SingleVendorInputReaderTest extends LocalSparkBase with Readings {

    Feature("Should filter by vendor on input") {
        import spark.implicits._

        implicit val enc1 = Encoders.bean(classOf[GreenTripRecord])
        implicit val enc2 = Encoders.bean(classOf[YellowTripRecord])

        Scenario("Should build vendor dimension list for green taxi") {
            Given("input params")
            val targetVendor = 1L
            val otherVendor = 2L
            val vendorColumnName = GreenTripRecord.EntityNames.VENDOR_ID

            And("Mock setup")
            val greenTaxiReader = mock[GenericTaxiReader[GreenTripRecord]](Mockito.withSettings().serializable())

            val greenTaxies = Seq(
                greenSample().copy(vendorId = otherVendor.toOpt),
                greenSample().copy(vendorId = targetVendor.toOpt),
                greenSample().copy(vendorId = 4L.toOpt),
                greenSample().copy(vendorId = 5L.toOpt)
            ).toDS()

            Mockito.when(greenTaxiReader.readInput(spark)).thenReturn(greenTaxies)

            val expected = Seq(
                greenSample().copy(vendorId = targetVendor.toOpt)
            ).toDS()

            And("Subject")
            val subject = new SingleVendorInputReader(vendorColumnName, targetVendor, greenTaxiReader, classOf[GreenTripRecord]);

            When("Input is read")
            val actual = subject.readInput(spark)
            Then("Target dimension is left")
            assertDatasetEquals(expected, actual)
        }

        Scenario("Should build vendor dimension list for yellow taxi") {
            Given("input params")
            val targetVendor = 1L
            val otherVendor = 2L
            val vendorColumnName = YellowTripRecord.EntityNames.VENDORID

            And("Mock setup")
            val yellowTaxiReader = mock[GenericTaxiReader[YellowTripRecord]](Mockito.withSettings().serializable())

            val yellowTaxies = Seq(
                yellowSample().copy(vendorID = otherVendor.toOpt),
                yellowSample().copy(vendorID = targetVendor.toOpt),
                yellowSample().copy(vendorID = 4L.toOpt),
                yellowSample().copy(vendorID = 5L.toOpt)
            ).toDS()

            Mockito.when(yellowTaxiReader.readInput(spark)).thenReturn(yellowTaxies)

            val expected = Seq(
                yellowSample().copy(vendorID = targetVendor.toOpt)
            ).toDS()

            And("Subject")
            val subject = new SingleVendorInputReader(vendorColumnName, targetVendor, yellowTaxiReader, classOf[YellowTripRecord]);

            When("Input is read")
            val actual = subject.readInput(spark)
            Then("Target dimension is left")
            assertDatasetEquals(expected, actual)
        }
    }

}
