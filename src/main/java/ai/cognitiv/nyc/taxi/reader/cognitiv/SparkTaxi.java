package ai.cognitiv.nyc.taxi.reader.cognitiv;

import ai.cognitiv.nyc.taxi.reader.cognitiv.config.CommandFactoryMethod;
import ai.cognitiv.nyc.taxi.reader.cognitiv.config.ConfigurationParams;
import ai.cognitiv.nyc.taxi.reader.cognitiv.config.DimensionsConfig;
import ai.cognitiv.nyc.taxi.reader.cognitiv.config.ReportingConfig;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;


@Slf4j
public final class SparkTaxi {

//  Please build a command line application that takes the last 2 months of data (include them in
//the project). We would like to be able to find aggregate information (min fare, max fare, count,
//sum of fares, and sum of toll fares, counts by payment type) of trips over the followingTr
//dimensions:
//1. Trips between certain times
//2. Trips between specific locations
//3. Trips from a certain vendor
//4. Trips for yellow taxis vs green taxis
//These dimensions should be able to be combined (i.e I'd like to know trips from a vendor over a
//certain time between these locations).

  public static void main(String[] args) {

    Map<String, String> curSysEnv = System.getenv();

    String greenTaxiLocation = null;
    String yellowTaxiLocation = null;

    log.warn("===== current env variables: \n" + curSysEnv);

    SparkConf conf = new SparkConf()
        .setAppName("taxi-analytic")
        .set("spark.driver.bindAddress", curSysEnv.getOrDefault("SPARK_DRIVER_HOST", "127.0.0.1"))
        .set(SQLConf.DATETIME_JAVA8API_ENABLED().key(), "true")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.master", curSysEnv.getOrDefault("SPARK_MASTER_URL", "local[1]"));

    log.info("Running spark in a client mode");
    conf.set("spark.submit.deployMode", "client");
    //conf.setMaster(curSysEnv.getOrDefault("SPARK_MASTER_URL", "spark://spark-master:7077"));
    greenTaxiLocation = curSysEnv.get("GREEN_TAXI");
    yellowTaxiLocation = curSysEnv.get("YELLOW_TAXI");

    log.info("current spark config to use:\n" + conf.toDebugString());
    try (JavaSparkContext sc = new JavaSparkContext(conf);
         SparkSession session = SparkSession.builder().sparkContext(sc.sc()).getOrCreate()) {


      LocalDateTime afterDt = LocalDateTime.of(2023, Month.OCTOBER, 20, 0, 0, 0);
      LocalDateTime beforeDt = LocalDateTime.of(2023, Month.NOVEMBER, 25, 0, 0, 0);
      Long pickUpLocationId = 166L;
      Long dropOffLocationId = 74L;
      boolean searchGreen = true;
      boolean searchYellow = true;

      DimensionsConfig dimensions = DimensionsConfig.builder()
          .start(afterDt)
          .end(beforeDt)
          .pickUpLocationId(pickUpLocationId)
          .dropOffLocationId(dropOffLocationId)
          .greenTaxi(searchGreen)
          .yellowTaxi(searchYellow)
          .build();

      ReportingConfig report = ReportingConfig.builder()
          .pickUpLocationId(pickUpLocationId)
          .dropOffLocationId(dropOffLocationId)
          .build();

      ConfigurationParams configConstructedFromCommandPromptOrFile = ConfigurationParams.builder()
          .yellowTaxisPath(yellowTaxiLocation)
          .greenTaxisPath(greenTaxiLocation)
          .dimensionsConfig(dimensions)
          .reportingConfig(report)
          .build();

      CommandFactoryMethod factory = new CommandFactoryMethod(configConstructedFromCommandPromptOrFile);
      Command command = factory.buildCommand(session);
      command.execute(session);
    }

  }
}