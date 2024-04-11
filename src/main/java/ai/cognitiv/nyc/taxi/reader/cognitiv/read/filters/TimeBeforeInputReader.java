package ai.cognitiv.nyc.taxi.reader.cognitiv.read.filters;

import ai.cognitiv.nyc.taxi.reader.cognitiv.read.TaxiReader;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class TimeBeforeInputReader<T> implements TaxiReader<T> {

  private final String searchedTimeColumnName;
  private final LocalDateTime searchedTimeValue;
  private final TaxiReader<T> decorated;
  private final Class<T> target;

  public TimeBeforeInputReader(String searchedTimeColumnName, LocalDateTime searchedTimeValue, TaxiReader<T> decorated, Class<T> target) {
    this.searchedTimeColumnName = searchedTimeColumnName;
    this.searchedTimeValue = searchedTimeValue;
    this.decorated = decorated;
    this.target = target;
  }

  @Override
  public Dataset<T> readInput(SparkSession session) {
    return decorated.readInput(session)
        .where(functions.col(searchedTimeColumnName).lt(Timestamp.valueOf(searchedTimeValue)))
        .as(Encoders.bean(target));
  }
}
