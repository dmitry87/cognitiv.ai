package ai.cognitiv.nyc.taxi.reader.cognitiv.reporting;

import ai.cognitiv.nyc.taxi.reader.cognitiv.model.AggregatedTripData;
import ai.cognitiv.nyc.taxi.reader.cognitiv.model.AnalyzedTripRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public interface ReportGenerator {

  Dataset<AggregatedTripData> generate(SparkSession session, Dataset<AnalyzedTripRecord> input);

}
