package ai.cognitiv.nyc.taxi.reader.cognitiv.read;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class EmptyTaxiReader<T> implements TaxiReader<T> {

  private final Class<T> target;

  public EmptyTaxiReader(Class<T> target) {
    this.target = target;
  }

  @Override
  public Dataset<T> readInput(SparkSession session) {
    return session.emptyDataset(Encoders.bean(target));
  }
}
