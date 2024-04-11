package ai.cognitiv.nyc.taxi.reader.cognitiv.read.filters;

import ai.cognitiv.nyc.taxi.reader.cognitiv.read.TaxiReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class NoFilterReader<T> implements TaxiReader<T> {

  private final TaxiReader<T> taxiReader;

  public NoFilterReader(TaxiReader<T> taxiReader) {
    this.taxiReader = taxiReader;
  }

  @Override
  public Dataset<T> readInput(SparkSession session) {
    return taxiReader.readInput(session);
  }
}
