package ai.cognitiv.nyc.taxi.reader.cognitiv.reporting;

import ai.cognitiv.nyc.taxi.reader.cognitiv.model.AggregatedTripData;
import ai.cognitiv.nyc.taxi.reader.cognitiv.model.AnalyzedTripRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class AggregatingReport implements ReportGenerator {

  @Override
  public Dataset<AggregatedTripData> generate(SparkSession session, Dataset<AnalyzedTripRecord> input) {
    Dataset<Row> aggregated = input
        .groupBy(functions.col(AnalyzedTripRecord.Entity.PAYMENT_TYPE).as(AggregatedTripData.Entity.PAYMENT_TYPE))
        .agg(functions.min(functions.col(AnalyzedTripRecord.Entity.FARE_AMOUNT)).as(AggregatedTripData.Entity.MIN_FARE),
            functions.max(functions.col(AnalyzedTripRecord.Entity.FARE_AMOUNT)).as(AggregatedTripData.Entity.MAX_FARE),
            functions.count(functions.col("*")).as(AggregatedTripData.Entity.COUNT),
            functions.sum(functions.col(AnalyzedTripRecord.Entity.FARE_AMOUNT)).as(AggregatedTripData.Entity.FARE_SUM),
            functions.sum(functions.col(AnalyzedTripRecord.Entity.TOLL_AMOUNT)).as(AggregatedTripData.Entity.TOLLS_SUM));
    return aggregated.as(Encoders.bean(AggregatedTripData.class));
  }
}
