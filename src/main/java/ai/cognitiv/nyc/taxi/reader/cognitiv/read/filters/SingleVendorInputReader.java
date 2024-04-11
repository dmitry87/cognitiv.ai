package ai.cognitiv.nyc.taxi.reader.cognitiv.read.filters;

import ai.cognitiv.nyc.taxi.reader.cognitiv.read.TaxiReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class SingleVendorInputReader<T> implements TaxiReader<T> {

  private final String vendorColumnName;
  private final Long searchedVendor;
  private final TaxiReader<T> decorated;
  private final Class<T> target;

  public SingleVendorInputReader(String vendorColumnName, Long searchedVendor, TaxiReader<T> decorated, Class<T> target) {
    this.vendorColumnName = vendorColumnName;
    this.searchedVendor = searchedVendor;
    this.decorated = decorated;
    this.target = target;
  }

  @Override
  public Dataset<T> readInput(SparkSession session) {
    Dataset<T> recDs = decorated.readInput(session);
    return recDs
        .where(functions.col(vendorColumnName).equalTo(searchedVendor))
        .as(Encoders.bean(target));
  }
}
