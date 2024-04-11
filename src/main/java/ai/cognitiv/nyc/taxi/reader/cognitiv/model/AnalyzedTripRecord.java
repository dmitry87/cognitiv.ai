package ai.cognitiv.nyc.taxi.reader.cognitiv.model;

import java.io.Serializable;
import java.sql.Timestamp;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor(staticName = "of")
@NoArgsConstructor
public class AnalyzedTripRecord implements Serializable {

  private Long vendorId;
  private Timestamp pickUpDateTime;
  private Timestamp dropOffDateTime;
  private Double fareAmount;
  private Double tollAmount;
  private Double paymentType;
  private Long pickUpLocationId;
  private Long dropOffLocationId;
  private String color;

  public static class Entity {

    private Entity() {
    }

    public static final String VENDOR_ID = "vendorId";
    public static final String PICK_UP_DATE_TIME = "pickUpDateTime";
    public static final String DROP_OFF_DATE_TIME = "dropOffDateTime";
    public static final String FARE_AMOUNT = "fareAmount";
    public static final String TOLL_AMOUNT = "tollAmount";
    public static final String PAYMENT_TYPE = "paymentType";
    public static final String PICK_UP_LOCATION_ID = "pickUpLocationId";
    public static final String DROP_OFF_LOCATION_ID = "dropOffLocationId";
    public static final String COLOR = "color";
  }
}
