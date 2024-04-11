package ai.cognitiv.nyc.taxi.reader.cognitiv.e2e

import ai.cognitiv.nyc.taxi.reader.cognitiv.LocalSparkBase
import ai.cognitiv.nyc.taxi.reader.cognitiv.config.{CommandFactoryMethod, ConfigurationParams, DimensionsConfig, ReportingConfig}
import ai.cognitiv.nyc.taxi.reader.cognitiv.model.{GreenTripRecord, YellowTripRecord}
import org.apache.spark.sql.{Encoder, Encoders}

import java.time.LocalDateTime

class E2EReportsTest extends LocalSparkBase {

    Feature("E2E reports testing on input files") {
        import spark.implicits._
        implicit val encoder: Encoder[YellowTripRecord] = Encoders.bean(classOf[YellowTripRecord])
        implicit val encoder2: Encoder[GreenTripRecord] = Encoders.bean(classOf[GreenTripRecord])

        Scenario("Should import and aggregate on all data when all taxis imported") {
            Given("Input for the files")
            val yellowTaxiPath = this.getClass.getResource("/samples/generictaxireader10/yellow_tripdata_sample.parquet").getPath
            val yellowTaxiPathArray = yellowTaxiPath
            val greenTaxiPath = this.getClass.getResource("/samples/generictaxireader10/green_tripdata_sample.parquet").getPath
            val greenTaxiPathArray = greenTaxiPath
            And("Config")

            val mainConf = ConfigurationParams.builder()
                .dimensionsConfig(DimensionsConfig.builder()
                    .greenTaxi(true)
                    .yellowTaxi(true)
                    .build())
                .reportingConfig(ReportingConfig.builder().build())
                .greenTaxisPath(greenTaxiPathArray)
                .yellowTaxisPath(yellowTaxiPathArray)
                .build()

            And("Build command")
            val command = new CommandFactoryMethod(mainConf).buildCommand(spark);

            Then("program is run")
            noException should be thrownBy command.execute(spark);
            And("Verify file output contains expected contents")
            // TODO: automate verification with output report consuming
        }

        Scenario("Should import and aggregate on all taxis when pickup and drop off dimension selected") {
            Given("Input for the files")
            val yellowTaxiPath = this.getClass.getResource("/samples/generictaxireader10/yellow_tripdata_sample.parquet").getPath
            val yellowTaxiPathArray = yellowTaxiPath
            val greenTaxiPath = this.getClass.getResource("/samples/generictaxireader10/green_tripdata_sample.parquet").getPath
            val greenTaxiPathArray = greenTaxiPath
            And("Config")

            val mainConf = ConfigurationParams.builder()
                .dimensionsConfig(DimensionsConfig.builder()
                    .greenTaxi(true)
                    .yellowTaxi(true)
                    .pickUpLocationId(50)
                    .dropOffLocationId(50)
                    .build())
                .reportingConfig(ReportingConfig.builder().build())
                .greenTaxisPath(greenTaxiPathArray)
                .yellowTaxisPath(yellowTaxiPathArray)
                .build()

            And("Build command")
            val command = new CommandFactoryMethod(mainConf).buildCommand(spark);

            Then("program is run")
            noException should be thrownBy command.execute(spark);
            And("Verify file output contains expected contents")
            // TODO: automate verification with output report consuming (should have 1 record)
        }

        Scenario(s"Should import and aggregate on all taxis when trips between '2023-09-30 17:22:33' and '2023-09-30 17:39:47'") {
            Given("Input for the files")
            val yellowTaxiPath = this.getClass.getResource("/samples/generictaxireader10/yellow_tripdata_sample.parquet").getPath
            val yellowTaxiPathArray = yellowTaxiPath
            val greenTaxiPath = this.getClass.getResource("/samples/generictaxireader10/green_tripdata_sample.parquet").getPath
            val greenTaxiPathArray = greenTaxiPath
            And("Config")
            val earliestPickUpTime = LocalDateTime.parse("2023-09-30T17:22:33")
            val latestPickUpTime = LocalDateTime.parse("2023-09-30T17:39:47")

            val mainConf = ConfigurationParams.builder()
                .dimensionsConfig(DimensionsConfig.builder()
                    .greenTaxi(true)
                    .yellowTaxi(true)
                    .start(earliestPickUpTime)
                    .end(latestPickUpTime)
                    .build())
                .reportingConfig(ReportingConfig.builder().build())
                .greenTaxisPath(greenTaxiPathArray)
                .yellowTaxisPath(yellowTaxiPathArray)
                .build()

            And("Build command")
            val command = new CommandFactoryMethod(mainConf).buildCommand(spark);

            Then("program is run")
            noException should be thrownBy command.execute(spark);
            And("Verify file output contains expected contents")
            // TODO: automate verification with output report consuming
        }

        Scenario(s"Should import and aggregate on all taxis when trips between '2023-09-30 17:22:33' and '2023-09-30 17:39:47' for vendor 1") {
            Given("Input for the files")
            val yellowTaxiPath = this.getClass.getResource("/samples/generictaxireader10/yellow_tripdata_sample.parquet").getPath
            val yellowTaxiPathArray = yellowTaxiPath
            val greenTaxiPath = this.getClass.getResource("/samples/generictaxireader10/green_tripdata_sample.parquet").getPath
            val greenTaxiPathArray = greenTaxiPath
            And("Config")
            val earliestPickUpTime = LocalDateTime.parse("2023-09-30T17:22:33")
            val latestPickUpTime = LocalDateTime.parse("2023-09-30T17:39:47")

            val targetVendorId = 1

            val mainConf = ConfigurationParams.builder()
                .dimensionsConfig(DimensionsConfig.builder()
                    .greenTaxi(true)
                    .yellowTaxi(true)
                    .start(earliestPickUpTime)
                    .end(latestPickUpTime)
                    .vendorId(targetVendorId)
                    .build())
                .reportingConfig(ReportingConfig.builder()
                    .build())
                .greenTaxisPath(greenTaxiPathArray)
                .yellowTaxisPath(yellowTaxiPathArray)
                .build()

            And("Build command")
            val command = new CommandFactoryMethod(mainConf).buildCommand(spark);

            Then("program is run")
            noException should be thrownBy command.execute(spark);
            And("Verify file output contains expected contents")
            // TODO: automate verification with output report consuming
        }

        Scenario(s"Should import and aggregate on Green taxis when trips between '2023-09-30 17:22:33' and '2023-09-30 17:39:47' for locations") {
            Given("Input for the files")
            val yellowTaxiPath = this.getClass.getResource("/samples/generictaxireader10/yellow_tripdata_sample.parquet").getPath
            val yellowTaxiPathArray = yellowTaxiPath
            val greenTaxiPath = this.getClass.getResource("/samples/generictaxireader10/green_tripdata_sample.parquet").getPath
            val greenTaxiPathArray = greenTaxiPath
            And("Config")
            val earliestPickUpTime = LocalDateTime.parse("2023-09-30T17:22:33")
            val latestPickUpTime = LocalDateTime.parse("2023-09-30T17:39:47")

            And("pick up location id = 74 and drop off location id = 236")
            val pickupLocationId = 74
            val dropOffLocationId = 236

            val mainConf = ConfigurationParams.builder()
                .dimensionsConfig(DimensionsConfig.builder()
                    .greenTaxi(true)
                    .start(earliestPickUpTime)
                    .end(latestPickUpTime)
                    .build())
                .reportingConfig(ReportingConfig.builder()
                    .pickUpLocationId(pickupLocationId)
                    .dropOffLocationId(dropOffLocationId)
                    .build())
                .greenTaxisPath(greenTaxiPathArray)
                .yellowTaxisPath(yellowTaxiPathArray)
                .build()

            And("Build command")
            val command = new CommandFactoryMethod(mainConf).buildCommand(spark);

            Then("program is run")
            noException should be thrownBy command.execute(spark);
            And("Verify file output contains expected contents")
            // TODO: automate verification with output report consuming
        }

        Scenario(s"Should import and aggregate on Yellow taxis for locations") {
            Given("Input for the files")
            val yellowTaxiPath = this.getClass.getResource("/samples/generictaxireader10/yellow_tripdata_sample.parquet").getPath
            val yellowTaxiPathArray = yellowTaxiPath
            val greenTaxiPath = this.getClass.getResource("/samples/generictaxireader10/green_tripdata_sample.parquet").getPath
            val greenTaxiPathArray = greenTaxiPath
            And("Config")

            And("pick up location id = 168 and drop off location id = 168")
            val pickupLocationId = 168
            val dropOffLocationId = 168

            val mainConf = ConfigurationParams.builder()
                .dimensionsConfig(DimensionsConfig.builder()
                    .yellowTaxi(true)
                    .build())
                .reportingConfig(ReportingConfig.builder()
                    .pickUpLocationId(pickupLocationId)
                    .dropOffLocationId(dropOffLocationId)
                    .build())
                .greenTaxisPath(greenTaxiPathArray)
                .yellowTaxisPath(yellowTaxiPathArray)
                .build()

            And("Build command")
            val command = new CommandFactoryMethod(mainConf).buildCommand(spark);

            Then("program is run")
            noException should be thrownBy command.execute(spark);
            And("Verify file output contains expected contents")
            // TODO: automate verification with output report consuming
        }
    }

}
