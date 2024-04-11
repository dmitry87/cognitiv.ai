package ai.cognitiv.nyc.taxi.reader.cognitiv.read;

import static org.apache.spark.sql.functions.col;

import java.util.Map;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class GenericTaxiReader<T> implements TaxiReader<T> {

  private final String[] input;
  private final Map<String, String> mapping;
  private final Class<T> target;

  public GenericTaxiReader(String[] input, Map<String, String> mapping, Class<T> target) {
    this.input = input;
    this.mapping = mapping;
    this.target = target;
  }

  @Override
  public Dataset<T> readInput(SparkSession session) {
    Column[] columnMapping = mapping.entrySet().stream()
        .map(entry -> col(entry.getKey()).as(entry.getValue()))
        .toArray(Column[]::new);

    Dataset<T> couldBeCachedResult = session.read().parquet(input)
        .select(columnMapping)
        .as(Encoders.bean(target));

    return couldBeCachedResult;

  }
}
