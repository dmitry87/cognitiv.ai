package ai.cognitiv.nyc.taxi.reader.cognitiv.read

import ai.cognitiv.nyc.taxi.reader.cognitiv.ImplicitHelpers._
import ai.cognitiv.nyc.taxi.reader.cognitiv.LocalSparkBase
import ai.cognitiv.nyc.taxi.reader.cognitiv.model.{GreenTripRecord, YellowTripRecord}
import org.apache.spark.sql.{Dataset, Encoder, Encoders}


class GenericTaxiReaderTest extends LocalSparkBase with Readings {

    Feature("Reads Parquet files") {

        import spark.implicits._
        implicit val encoder: Encoder[YellowTripRecord] = Encoders.bean(classOf[YellowTripRecord])
        implicit val encoder2: Encoder[GreenTripRecord] = Encoders.bean(classOf[GreenTripRecord])


        Scenario("Reader reads yellow taxi file") {
            Given("Yellow tax file path")
            val path = this.getClass.getResource("/samples/generictaxireader3/yellow_tripdata_sample.parquet").getPath
            val pathArray = Array(path)

            And("reader")
            val subject = new GenericTaxiReader(pathArray, YellowTripRecord.MAPPING, classOf[YellowTripRecord])

            And("Expected to contain")

            val expected: Dataset[YellowTripRecord] = Seq(
                yellowSample(),
                yellowSample().copy(tpepPickUpDatetime = "2023-09-30T17:23:24.00".toOpt, tpepDropOffDatetime = "2023-09-30T17:23:47.00".toOpt),
                yellowSample().copy(tpepPickUpDatetime = "2023-09-30T17:21:18.00".toOpt, tpepDropOffDatetime = "2023-09-30T17:27:31.00".toOpt, tripDistance = 0.9.toOpt, pickUpLocationId = 161L.toOpt, dropOffLocationId = 186L.toOpt, paymentType = 1L.toOpt, fareAmount = 6.5.toOpt, extra = 3.5.toOpt, tipAmount = 2.9.toOpt, totalAmount = 14.4.toOpt, congestionSurcharge = 2.5.toOpt)
            ).toDS()

            When("File is read")
            val result = subject.readInput(spark)

            Then("Should contain expected records")
            assertDatasetEquals(expected, result)

        }

        Scenario("Reader reads green taxi file") {
            Given("Yellow tax file path")
            val path = this.getClass.getResource("/samples/generictaxireader3/green_tripdata_sample.parquet").getPath
            val pathArray = Array(path)

            And("reader")
            val subject = new GenericTaxiReader(pathArray, GreenTripRecord.MAPPING, classOf[GreenTripRecord])

            And("Expected to contain")
            val expected: Dataset[GreenTripRecord] = Seq(
                greenSample(),
                greenSample().copy(lpepPickUpDatetime = "2023-09-30T18:00:16.00".toOpt, lpepDropOffDatetime = "2023-09-30T18:06:13.00".toOpt, pickUpLocationId = 74L.toOpt, dropOffLocationId = 42L.toOpt, tripDistance = 0.89.toOpt, fareAmount = 7.9.toOpt, tipAmount = 0D.toOpt, totalAmount = 10.4.toOpt, paymentType = 2D.toOpt),
                greenSample().copy(lpepPickUpDatetime = "2023-09-30T17:51:52.0".toOpt, lpepDropOffDatetime = "2023-09-30T18:00:32.0".toOpt, pickUpLocationId = 83L.toOpt, dropOffLocationId = 129L.toOpt, tripDistance = 2.38.toOpt, fareAmount = 13.5.toOpt, tipAmount = 0D.toOpt, totalAmount = 16.0.toOpt, paymentType = 2D.toOpt)
            ).toDS()

            When("File is read")
            val result: Dataset[GreenTripRecord] = subject.readInput(spark)

            Then("Should contain expected records")
            assertDatasetEquals(expected = expected, result)
        }

    }

}
