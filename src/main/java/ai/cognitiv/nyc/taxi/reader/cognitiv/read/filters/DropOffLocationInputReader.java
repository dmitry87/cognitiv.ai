package ai.cognitiv.nyc.taxi.reader.cognitiv.read.filters;

import ai.cognitiv.nyc.taxi.reader.cognitiv.read.TaxiReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class DropOffLocationInputReader<T> implements TaxiReader<T> {

  private final String dropOffLocationColumnName;
  private final Long locationId;
  private final TaxiReader<T> decorated;
  private final Class<T> target;

  public DropOffLocationInputReader(String dropOffLocationColumnName, Long locationId, TaxiReader<T> decorated, Class<T> target) {
    this.dropOffLocationColumnName = dropOffLocationColumnName;
    this.locationId = locationId;
    this.decorated = decorated;
    this.target = target;
  }

  @Override
  public Dataset<T> readInput(SparkSession session) {
    return decorated.readInput(session)
        .where(functions.col(dropOffLocationColumnName).equalTo(locationId))
        .as(Encoders.bean(target));
  }
}
