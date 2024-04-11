package ai.cognitiv.nyc.taxi.reader.cognitiv.model;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;

@Data
public class YellowTripRecord implements Serializable {

  public static final String TABLE_NAME = "yellow.trips";

  private Long vendorID;
  private Timestamp tpepPickUpDatetime;
  private Timestamp tpepDropOffDatetime;
  private Double passengerCount;
  private Double tripDistance;
  private Double rateCodeId;
  private String storeAndFwdFlag;
  private Long pickUpLocationId;
  private Long dropOffLocationId;
  private Long paymentType;
  private Double fareAmount;
  private Double extra;
  private Double mtaTax;
  private Double tipAmount;
  private Double tollsAmount;
  private Double improvementSurcharge;
  private Double totalAmount;
  private Double congestionSurcharge;
  private Double airportFee;

  public static Map<String, String> MAPPING;

    static {
        MAPPING = new HashMap<>();
        MAPPING.put(ColumnNames.VENDORID, EntityNames.VENDORID);
        MAPPING.put(ColumnNames.TPEP_PICKUP_DATETIME, EntityNames.TPEP_PICKUP_DATETIME);
        MAPPING.put(ColumnNames.TPEP_DROPOFF_DATETIME, EntityNames.TPEP_DROPOFF_DATETIME);
        MAPPING.put(ColumnNames.PASSENGER_COUNT, EntityNames.PASSENGER_COUNT);
        MAPPING.put(ColumnNames.TRIP_DISTANCE, EntityNames.TRIP_DISTANCE);
        MAPPING.put(ColumnNames.RATECODEID, EntityNames.RATECODEID);
        MAPPING.put(ColumnNames.STORE_AND_FWD_FLAG, EntityNames.STORE_AND_FWD_FLAG);
        MAPPING.put(ColumnNames.PULOCATIONID, EntityNames.PULOCATIONID);
        MAPPING.put(ColumnNames.DOLOCATIONID, EntityNames.DOLOCATIONID);
        MAPPING.put(ColumnNames.PAYMENT_TYPE, EntityNames.PAYMENT_TYPE);
        MAPPING.put(ColumnNames.FARE_AMOUNT, EntityNames.FARE_AMOUNT);
        MAPPING.put(ColumnNames.EXTRA, EntityNames.EXTRA);
        MAPPING.put(ColumnNames.MTA_TAX, EntityNames.MTA_TAX);
        MAPPING.put(ColumnNames.TIP_AMOUNT, EntityNames.TIP_AMOUNT);
        MAPPING.put(ColumnNames.TOLLS_AMOUNT, EntityNames.TOLLS_AMOUNT);
        MAPPING.put(ColumnNames.IMPROVEMENT_SURCHARGE, EntityNames.IMPROVEMENT_SURCHARGE);
        MAPPING.put(ColumnNames.TOTAL_AMOUNT, EntityNames.TOTAL_AMOUNT);
        MAPPING.put(ColumnNames.CONGESTION_SURCHARGE, EntityNames.CONGESTION_SURCHARGE);
        MAPPING.put(ColumnNames.AIRPORT_FEE, EntityNames.AIRPORT_FEE);
    }


    private static class ColumnNames {

    static final String VENDORID = "VendorID";
    static final String TPEP_PICKUP_DATETIME = "tpep_pickup_datetime";
    static final String TPEP_DROPOFF_DATETIME = "tpep_dropoff_datetime";
    static final String PASSENGER_COUNT = "passenger_count";
    static final String TRIP_DISTANCE = "trip_distance";
    static final String RATECODEID = "RatecodeID";
    static final String STORE_AND_FWD_FLAG = "store_and_fwd_flag";
    static final String PULOCATIONID = "PULocationID";
    static final String DOLOCATIONID = "DOLocationID";
    static final String PAYMENT_TYPE = "payment_type";
    static final String FARE_AMOUNT = "fare_amount";
    static final String EXTRA = "extra";
    static final String MTA_TAX = "mta_tax";
    static final String TIP_AMOUNT = "tip_amount";
    static final String TOLLS_AMOUNT = "tolls_amount";
    static final String IMPROVEMENT_SURCHARGE = "improvement_surcharge";
    static final String TOTAL_AMOUNT = "total_amount";
    static final String CONGESTION_SURCHARGE = "congestion_surcharge";
    static final String AIRPORT_FEE = "airport_fee";
  }

  public static class EntityNames {

    private EntityNames() {
    }

    public static final String VENDORID = "vendorID";
    public static final String TPEP_PICKUP_DATETIME = "tpepPickUpDatetime";
    public static final String TPEP_DROPOFF_DATETIME = "tpepDropOffDatetime";
    public static final String PASSENGER_COUNT = "passengerCount";
    public static final String TRIP_DISTANCE = "tripDistance";
    public static final String RATECODEID = "rateCodeId";
    public static final String STORE_AND_FWD_FLAG = "storeAndFwdFlag";
    public static final String PULOCATIONID = "pickUpLocationId";
    public static final String DOLOCATIONID = "dropOffLocationId";
    public static final String PAYMENT_TYPE = "paymentType";
    public static final String FARE_AMOUNT = "fareAmount";
    public static final String EXTRA = "extra";
    public static final String MTA_TAX = "mtaTax";
    public static final String TIP_AMOUNT = "tipAmount";
    public static final String TOLLS_AMOUNT = "tollsAmount";
    public static final String IMPROVEMENT_SURCHARGE = "improvementSurcharge";
    public static final String TOTAL_AMOUNT = "totalAmount";
    public static final String CONGESTION_SURCHARGE = "congestionSurcharge";
    public static final String AIRPORT_FEE = "airportFee";

  }
}
