package ai.cognitiv.nyc.taxi.reader.cognitiv;

import ai.cognitiv.nyc.taxi.reader.cognitiv.model.AnalyzedTripRecord;
import ai.cognitiv.nyc.taxi.reader.cognitiv.read.TaxiReader;
import ai.cognitiv.nyc.taxi.reader.cognitiv.reporting.ReportGenerator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class Command {

  private static final int MAX_AMOUNT_OF_PAYMENT_TYPES = 100;

  private final TaxiReader<AnalyzedTripRecord> taxiReader;
  private final ReportGenerator reportGenerator;

  public Command(TaxiReader<AnalyzedTripRecord> taxiReader, ReportGenerator reportGenerator) {
    this.taxiReader = taxiReader;
    this.reportGenerator = reportGenerator;
  }

  public void execute(SparkSession session) {
    Dataset<AnalyzedTripRecord> input = taxiReader.readInput(session);
    reportGenerator.generate(session, input)
        .show(MAX_AMOUNT_OF_PAYMENT_TYPES);
  }

}
