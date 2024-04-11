package ai.cognitiv.nyc.taxi.reader.cognitiv.model;

import java.io.Serializable;
import lombok.Data;


@Data
public class AggregatedTripData implements Serializable {

  //We would like to be able to find aggregate information (min fare, max fare, count,
////sum of fares, and sum of toll fares, counts by payment type)
  private Double minFare;
  private Double maxFare;
  private Long count;
  private Double fareSum;
  private Double tollsSum;
  private Double paymentType;


  public static class Entity {

    private Entity() {
    }

    public static final String MIN_FARE = "minFare";
    public static final String MAX_FARE = "maxFare";
    public static final String COUNT = "count";
    public static final String FARE_SUM = "fareSum";
    public static final String TOLLS_SUM = "tollsSum";
    public static final String PAYMENT_TYPE = "paymentType";
  }
}
