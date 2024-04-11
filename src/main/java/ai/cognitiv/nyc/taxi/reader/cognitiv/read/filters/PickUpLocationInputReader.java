package ai.cognitiv.nyc.taxi.reader.cognitiv.read.filters;

import ai.cognitiv.nyc.taxi.reader.cognitiv.read.TaxiReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class PickUpLocationInputReader<T> implements TaxiReader<T> {

  private final String pickUpLocationColumnName;
  private final Long locationId;
  private final TaxiReader<T> decorated;
  private final Class<T> target;

  public PickUpLocationInputReader(String pickUpLocationColumnName, Long locationId, TaxiReader<T> decorated, Class<T> target) {
    this.pickUpLocationColumnName = pickUpLocationColumnName;
    this.locationId = locationId;
    this.decorated = decorated;
    this.target = target;
  }

  @Override
  public Dataset<T> readInput(SparkSession session) {
    return decorated.readInput(session)
        .where(functions.col(pickUpLocationColumnName).equalTo(locationId))
        .as(Encoders.bean(target));
  }
}
