package ai.cognitiv.nyc.taxi.reader.cognitiv.model;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;

@Data
public class GreenTripRecord implements Serializable {

  public static final String TABLE_NAME = "green.trips";

  private Long vendorId;
  private Timestamp lpepPickUpDatetime;
  private Timestamp lpepDropOffDatetime;
  private String storeAndFwdFlag;
  private Double ratecodeID;
  private Long pickUpLocationId;
  private Long dropOffLocationId;
  private Double passengerCount;
  private Double tripDistance;
  private Double fareAmount;
  private Double extra;
  private Double mtaTax;
  private Double tipAmount;
  private Double tollsAmount;
  private Double ehailFee;
  private Double improvementSurcharge;
  private Double totalAmount;
  private Double paymentType;
  private Double tripType;

  public static Map<String, String> MAPPING;

    static {
        MAPPING = new HashMap<>();
        MAPPING.put(ColumnNames.VENDOR_ID, EntityNames.VENDOR_ID);
        MAPPING.put(ColumnNames.LPEP_PICKUP_DATETIME, EntityNames.LPEP_PICKUP_DATETIME);
        MAPPING.put(ColumnNames.LPEP_DROPOFF_DATETIME, EntityNames.LPEP_DROPOFF_DATETIME);
        MAPPING.put(ColumnNames.STORE_AND_FWD_FLAG, EntityNames.STORE_AND_FWD_FLAG);
        MAPPING.put(ColumnNames.RATECODE_ID, EntityNames.RATECODE_ID);
        MAPPING.put(ColumnNames.PU_LOCATION_ID, EntityNames.PU_LOCATION_ID);
        MAPPING.put(ColumnNames.DO_LOCATION_ID, EntityNames.DO_LOCATION_ID);
        MAPPING.put(ColumnNames.PASSENGER_COUNT, EntityNames.PASSENGER_COUNT);
        MAPPING.put(ColumnNames.TRIP_DISTANCE, EntityNames.TRIP_DISTANCE);
        MAPPING.put(ColumnNames.FARE_AMOUNT, EntityNames.FARE_AMOUNT);
        MAPPING.put(ColumnNames.EXTRA, EntityNames.EXTRA);
        MAPPING.put(ColumnNames.MTA_TAX, EntityNames.MTA_TAX);
        MAPPING.put(ColumnNames.TIP_AMOUNT, EntityNames.TIP_AMOUNT);
        MAPPING.put(ColumnNames.TOLLS_AMOUNT, EntityNames.TOLLS_AMOUNT);
        MAPPING.put(ColumnNames.EHAIL_FEE, EntityNames.EHAIL_FEE);
        MAPPING.put(ColumnNames.IMPROVEMENT_SURCHARGE, EntityNames.IMPROVEMENT_SURCHARGE);
        MAPPING.put(ColumnNames.TOTAL_AMOUNT, EntityNames.TOTAL_AMOUNT);
        MAPPING.put(ColumnNames.TRIP_TYPE, EntityNames.TRIP_TYPE);
        MAPPING.put(ColumnNames.PAYMENT_TYPE, EntityNames.PAYMENT_TYPE);
        MAPPING.put(ColumnNames.CONGESTION_SURCHARGE, EntityNames.CONGESTION_SURCHARGE);
    }


    private static class ColumnNames {

    static final String VENDOR_ID = "VendorID";
    static final String LPEP_PICKUP_DATETIME = "lpep_pickup_datetime";
    static final String LPEP_DROPOFF_DATETIME = "lpep_dropoff_datetime";
    static final String STORE_AND_FWD_FLAG = "store_and_fwd_flag";
    static final String RATECODE_ID = "RatecodeID";
    static final String PU_LOCATION_ID = "PULocationID";
    static final String DO_LOCATION_ID = "DOLocationID";
    static final String PASSENGER_COUNT = "passenger_count";
    static final String TRIP_DISTANCE = "trip_distance";
    static final String FARE_AMOUNT = "fare_amount";
    static final String EXTRA = "extra";
    static final String MTA_TAX = "mta_tax";
    static final String TIP_AMOUNT = "tip_amount";
    static final String TOLLS_AMOUNT = "tolls_amount";
    static final String EHAIL_FEE = "ehail_fee";
    static final String IMPROVEMENT_SURCHARGE = "improvement_surcharge";
    static final String TOTAL_AMOUNT = "total_amount";
    static final String PAYMENT_TYPE = "payment_type";
    static final String TRIP_TYPE = "trip_type";
    static final String CONGESTION_SURCHARGE = "congestion_surcharge";
  }

  public static class EntityNames {

    private EntityNames() {
    }

    public static final String VENDOR_ID = "vendorId";
    public static final String LPEP_PICKUP_DATETIME = "lpepPickUpDatetime";
    public static final String LPEP_DROPOFF_DATETIME = "lpepDropOffDatetime";
    public static final String STORE_AND_FWD_FLAG = "storeAndFwdFlag";
    public static final String RATECODE_ID = "ratecodeID";
    public static final String PU_LOCATION_ID = "pickUpLocationId";
    public static final String DO_LOCATION_ID = "dropOffLocationId";
    public static final String PASSENGER_COUNT = "passengerCount";
    public static final String TRIP_DISTANCE = "tripDistance";
    public static final String FARE_AMOUNT = "fareAmount";
    public static final String EXTRA = "extra";
    public static final String MTA_TAX = "mtaTax";
    public static final String TIP_AMOUNT = "tipAmount";
    public static final String TOLLS_AMOUNT = "tollsAmount";
    public static final String EHAIL_FEE = "ehailFee";
    public static final String IMPROVEMENT_SURCHARGE = "improvementSurcharge";
    public static final String TOTAL_AMOUNT = "totalAmount";
    public static final String PAYMENT_TYPE = "paymentType";
    public static final String TRIP_TYPE = "tripType";
    public static final String CONGESTION_SURCHARGE = "congestion_surcharge";

  }
}
