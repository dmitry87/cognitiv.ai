package ai.cognitiv.nyc.taxi.reader.cognitiv.read;

import java.io.Serializable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public interface TaxiReader<T> extends Serializable {

  Dataset<T> readInput(SparkSession session);

}
