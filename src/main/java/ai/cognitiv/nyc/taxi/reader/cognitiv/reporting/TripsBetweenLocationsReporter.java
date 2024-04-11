package ai.cognitiv.nyc.taxi.reader.cognitiv.reporting;

import ai.cognitiv.nyc.taxi.reader.cognitiv.model.AggregatedTripData;
import ai.cognitiv.nyc.taxi.reader.cognitiv.model.AnalyzedTripRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class TripsBetweenLocationsReporter implements ReportGenerator {

  private final Long pickUpLocationId;
  private final Long dropOffLocationId;

  public TripsBetweenLocationsReporter(Long pickUpLocationId, Long dropOffLocationId) {
    this.pickUpLocationId = pickUpLocationId;
    this.dropOffLocationId = dropOffLocationId;
  }


  public Dataset<AggregatedTripData> generate(SparkSession session, Dataset<AnalyzedTripRecord> input) {
    Dataset<Row> aggregated = input.where(
            functions.col(AnalyzedTripRecord.Entity.PICK_UP_LOCATION_ID).equalTo(pickUpLocationId)
                .and(functions.col(AnalyzedTripRecord.Entity.DROP_OFF_LOCATION_ID).equalTo(dropOffLocationId))
        ).groupBy(functions.col(AnalyzedTripRecord.Entity.PAYMENT_TYPE).as(AggregatedTripData.Entity.PAYMENT_TYPE))
        .agg(functions.min(functions.col(AnalyzedTripRecord.Entity.FARE_AMOUNT)).as(AggregatedTripData.Entity.MIN_FARE),
            functions.max(functions.col(AnalyzedTripRecord.Entity.FARE_AMOUNT)).as(AggregatedTripData.Entity.MAX_FARE),
            functions.count(functions.col("*")).as(AggregatedTripData.Entity.COUNT),
            functions.sum(functions.col(AnalyzedTripRecord.Entity.FARE_AMOUNT)).as(AggregatedTripData.Entity.FARE_SUM),
            functions.sum(functions.col(AnalyzedTripRecord.Entity.TOLL_AMOUNT)).as(AggregatedTripData.Entity.TOLLS_SUM));
    return aggregated.as(Encoders.bean(AggregatedTripData.class));

  }

}
