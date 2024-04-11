package ai.cognitiv.nyc.taxi.reader.cognitiv.read;

import ai.cognitiv.nyc.taxi.reader.cognitiv.model.AnalyzedTripRecord;
import ai.cognitiv.nyc.taxi.reader.cognitiv.model.GreenTripRecord;
import ai.cognitiv.nyc.taxi.reader.cognitiv.model.TaxiColor;
import ai.cognitiv.nyc.taxi.reader.cognitiv.model.YellowTripRecord;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class TripRecordUnificator implements TaxiReader<AnalyzedTripRecord> {

  private final TaxiReader<YellowTripRecord> yellowReader;
  private final TaxiReader<GreenTripRecord> greenReader;

  public TripRecordUnificator(TaxiReader<YellowTripRecord> yellowReader, TaxiReader<GreenTripRecord> greenReader) {
    this.yellowReader = yellowReader;
    this.greenReader = greenReader;
  }


  @Override
  public Dataset<AnalyzedTripRecord> readInput(SparkSession session) {
    Dataset<AnalyzedTripRecord> yellow = yellowReader.readInput(session)
        .map((MapFunction<YellowTripRecord, AnalyzedTripRecord>) this::convert, Encoders.bean(AnalyzedTripRecord.class));

    Dataset<AnalyzedTripRecord> green = greenReader.readInput(session)
        .map((MapFunction<GreenTripRecord, AnalyzedTripRecord>) this::convert, Encoders.bean(AnalyzedTripRecord.class));

    return yellow.unionAll(green);
  }

  AnalyzedTripRecord convert(YellowTripRecord yellow) {
    return AnalyzedTripRecord.of(
        yellow.getVendorID(),
        yellow.getTpepPickUpDatetime(),
        yellow.getTpepDropOffDatetime(),
        yellow.getFareAmount(),
        yellow.getTollsAmount(),
        yellow.getPaymentType().doubleValue(),
        yellow.getPickUpLocationId(),
        yellow.getDropOffLocationId(),
        TaxiColor.YELLOW.getColor())
        ;
  }

  AnalyzedTripRecord convert(GreenTripRecord green) {
    return AnalyzedTripRecord.of(
        green.getVendorId(),
        green.getLpepPickUpDatetime(),
        green.getLpepDropOffDatetime(),
        green.getFareAmount(),
        green.getTollsAmount(),
        green.getPaymentType(),
        green.getPickUpLocationId(),
        green.getDropOffLocationId(),
        TaxiColor.GREEN.getColor())
        ;
  }
}
