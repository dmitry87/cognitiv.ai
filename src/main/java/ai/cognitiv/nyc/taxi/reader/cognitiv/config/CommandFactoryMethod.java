package ai.cognitiv.nyc.taxi.reader.cognitiv.config;

import ai.cognitiv.nyc.taxi.reader.cognitiv.Command;
import ai.cognitiv.nyc.taxi.reader.cognitiv.model.AnalyzedTripRecord;
import ai.cognitiv.nyc.taxi.reader.cognitiv.model.GreenTripRecord;
import ai.cognitiv.nyc.taxi.reader.cognitiv.model.YellowTripRecord;
import ai.cognitiv.nyc.taxi.reader.cognitiv.read.EmptyTaxiReader;
import ai.cognitiv.nyc.taxi.reader.cognitiv.read.GenericTaxiReader;
import ai.cognitiv.nyc.taxi.reader.cognitiv.read.TaxiReader;
import ai.cognitiv.nyc.taxi.reader.cognitiv.read.TripRecordUnificator;
import ai.cognitiv.nyc.taxi.reader.cognitiv.read.filters.DropOffLocationInputReader;
import ai.cognitiv.nyc.taxi.reader.cognitiv.read.filters.NoFilterReader;
import ai.cognitiv.nyc.taxi.reader.cognitiv.read.filters.PickUpLocationInputReader;
import ai.cognitiv.nyc.taxi.reader.cognitiv.read.filters.SingleVendorInputReader;
import ai.cognitiv.nyc.taxi.reader.cognitiv.read.filters.TimeAfterInputReader;
import ai.cognitiv.nyc.taxi.reader.cognitiv.read.filters.TimeBeforeInputReader;
import ai.cognitiv.nyc.taxi.reader.cognitiv.reporting.AggregatingReport;
import ai.cognitiv.nyc.taxi.reader.cognitiv.reporting.ReportGenerator;
import ai.cognitiv.nyc.taxi.reader.cognitiv.reporting.TaxiColorReport;
import ai.cognitiv.nyc.taxi.reader.cognitiv.reporting.TripsBetweenLocationsReporter;
import ai.cognitiv.nyc.taxi.reader.cognitiv.reporting.TripsBetweenTimesReporter;
import ai.cognitiv.nyc.taxi.reader.cognitiv.reporting.VendorTrips;
import java.util.Objects;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class CommandFactoryMethod {

  private final ConfigurationParams config;

  public CommandFactoryMethod(ConfigurationParams config) {
    this.config = config;
  }

  public Command buildCommand(SparkSession spark) {

    DimensionsConfig dimConfig = config.getDimensionsConfig();

    TaxiReader<GreenTripRecord> greenReader = buildGreenTaxiReader(config.getGreenTaxisPath(), dimConfig);
    TaxiReader<YellowTripRecord> yellowReader = buildYellowTaxiReader(config.getYellowTaxisPath(), dimConfig);

    TaxiReader<GreenTripRecord> entriesAfterDateGreenReader = buildAfterDateGreenReader(greenReader, dimConfig);
    TaxiReader<YellowTripRecord> entriesAfterDateYellowReader = buildAfterDateYellowReader(yellowReader, dimConfig);

    TaxiReader<GreenTripRecord> entriesBeforeDateGreenReader = buildBeforeDateGreenReader(entriesAfterDateGreenReader, dimConfig);
    TaxiReader<YellowTripRecord> entriesBeforeDateYellowReader = buildBeforeDateYellowReader(entriesAfterDateYellowReader, dimConfig);

    TaxiReader<GreenTripRecord> pickupLocationGreenReader = buildPickUpLocationGreenReader(entriesBeforeDateGreenReader, dimConfig);
    TaxiReader<YellowTripRecord> pickupLocationYellowReader = buildPickUpLocationYellowReader(entriesBeforeDateYellowReader, dimConfig);

    TaxiReader<GreenTripRecord> dropOffLocationGreenReader = buildDropOffLocationGreenReader(pickupLocationGreenReader, dimConfig);
    TaxiReader<YellowTripRecord> dropOffLocationYellowReader = buildDropOffLocationYellowReader(pickupLocationYellowReader, dimConfig);

    TaxiReader<GreenTripRecord> vendorGreenReader = buildVendorGreenReader(dropOffLocationGreenReader, dimConfig);
    TaxiReader<YellowTripRecord> vendorYellowReader = buildVendorYellowReader(dropOffLocationYellowReader, dimConfig);

    TaxiReader<AnalyzedTripRecord> unificator = new TripRecordUnificator(vendorYellowReader, vendorGreenReader);

    ReportGenerator reporter = buildReportGenerator(config.getReportingConfig());
    return new Command(unificator, reporter);
  }

  private TaxiReader<YellowTripRecord> buildVendorYellowReader(TaxiReader<YellowTripRecord> dropOffLocationYellowReader, DimensionsConfig dimConfig) {
    if (Objects.isNull(dimConfig.getVendorId())) {
      return new NoFilterReader<>(dropOffLocationYellowReader);
    }

    return new SingleVendorInputReader<>(YellowTripRecord.EntityNames.VENDORID,
        dimConfig.getVendorId(), dropOffLocationYellowReader, YellowTripRecord.class);
  }

  private TaxiReader<GreenTripRecord> buildVendorGreenReader(TaxiReader<GreenTripRecord> dropOffLocationGreenReader, DimensionsConfig dimConfig) {
    if (Objects.isNull(dimConfig.getVendorId())) {
      return new NoFilterReader<>(dropOffLocationGreenReader);
    }

    return new SingleVendorInputReader<>(GreenTripRecord.EntityNames.VENDOR_ID,
        dimConfig.getVendorId(), dropOffLocationGreenReader, GreenTripRecord.class);
  }

  private ReportGenerator buildReportGenerator(ReportingConfig reportingConfig) {
    //    TODO: I'd move to map for real project at some point
    if (reportingConfig.getDropOffLocationId() != null && reportingConfig.getPickUpLocationId() != null) {
      return new TripsBetweenLocationsReporter(reportingConfig.getPickUpLocationId(), reportingConfig.getDropOffLocationId());
    } else if (reportingConfig.getEnd() != null && reportingConfig.getStart() != null) {
      return new TripsBetweenTimesReporter(reportingConfig.getStart(), reportingConfig.getEnd());
    } else if (reportingConfig.getVendorId() != null) {
      return new VendorTrips(reportingConfig.getVendorId());
    } else if (reportingConfig.getTaxiColor() != null) {
      return new TaxiColorReport(reportingConfig.getTaxiColor());
    }
    return new AggregatingReport();
  }

  private TaxiReader<YellowTripRecord> buildDropOffLocationYellowReader(TaxiReader<YellowTripRecord> pickupLocationYellowReader,
                                                                        DimensionsConfig config) {

    if (Objects.isNull(config.getDropOffLocationId())) {
      return new NoFilterReader<>(pickupLocationYellowReader);
    }

    return new DropOffLocationInputReader<>(YellowTripRecord.EntityNames.DOLOCATIONID,
        config.getDropOffLocationId(), pickupLocationYellowReader, YellowTripRecord.class);
  }

  private TaxiReader<GreenTripRecord> buildDropOffLocationGreenReader(TaxiReader<GreenTripRecord> pickupLocationGreenReader,
                                                                      DimensionsConfig config) {
    if (Objects.isNull(config.getDropOffLocationId())) {
      return new NoFilterReader<>(pickupLocationGreenReader);
    }

    return new DropOffLocationInputReader<>(GreenTripRecord.EntityNames.DO_LOCATION_ID,
        config.getDropOffLocationId(), pickupLocationGreenReader, GreenTripRecord.class);

  }

  private TaxiReader<YellowTripRecord> buildPickUpLocationYellowReader(TaxiReader<YellowTripRecord> entriesBeforeDateYellowReader,
                                                                       DimensionsConfig config) {
    if (Objects.isNull(config.getPickUpLocationId())) {
      return new NoFilterReader<>(entriesBeforeDateYellowReader);
    }
    return new PickUpLocationInputReader<>(YellowTripRecord.EntityNames.PULOCATIONID,
        config.getPickUpLocationId(), entriesBeforeDateYellowReader, YellowTripRecord.class);
  }

  private TaxiReader<GreenTripRecord> buildPickUpLocationGreenReader(TaxiReader<GreenTripRecord> entriesBeforeDateGreenReader,
                                                                     DimensionsConfig config) {
    if (Objects.isNull(config.getPickUpLocationId())) {
      return new NoFilterReader<>(entriesBeforeDateGreenReader);
    }
    return new PickUpLocationInputReader<>(GreenTripRecord.EntityNames.PU_LOCATION_ID,
        config.getPickUpLocationId(), entriesBeforeDateGreenReader, GreenTripRecord.class);
  }


  private TaxiReader<YellowTripRecord> buildBeforeDateYellowReader(TaxiReader<YellowTripRecord> afterDateYellowReader, DimensionsConfig config) {
    if (Objects.isNull(config.getEnd())) {
      return new NoFilterReader<>(afterDateYellowReader);
    }
    return new TimeBeforeInputReader<>(YellowTripRecord.EntityNames.TPEP_PICKUP_DATETIME, config.getEnd(),
        afterDateYellowReader, YellowTripRecord.class);
  }

  private TaxiReader<GreenTripRecord> buildBeforeDateGreenReader(TaxiReader<GreenTripRecord> afterDateGreenReader, DimensionsConfig config) {
    if (Objects.isNull(config.getEnd())) {
      return new NoFilterReader<>(afterDateGreenReader);
    }
    return new TimeBeforeInputReader<>(GreenTripRecord.EntityNames.LPEP_PICKUP_DATETIME, config.getEnd(),
        afterDateGreenReader, GreenTripRecord.class);
  }

  private TaxiReader<YellowTripRecord> buildAfterDateYellowReader(TaxiReader<YellowTripRecord> yellowReader, DimensionsConfig config) {
    if (Objects.isNull(config.getStart())) {
      return new NoFilterReader<>(yellowReader);
    }
    return new TimeAfterInputReader<>(YellowTripRecord.EntityNames.TPEP_PICKUP_DATETIME, config.getStart(),
        yellowReader, YellowTripRecord.class);
  }

  private TaxiReader<GreenTripRecord> buildAfterDateGreenReader(TaxiReader<GreenTripRecord> greenReader, DimensionsConfig config) {
    if (Objects.isNull(config.getStart())) {
      return new NoFilterReader<>(greenReader);
    }
    return new TimeAfterInputReader<>(GreenTripRecord.EntityNames.LPEP_PICKUP_DATETIME, config.getStart(),
        greenReader, GreenTripRecord.class);
  }

  private TaxiReader<YellowTripRecord> buildYellowTaxiReader(String yellowTaxisPath, DimensionsConfig config) {
    if (!config.isYellowTaxi()) {
      return new EmptyTaxiReader<>(YellowTripRecord.class);
    }

    String[] paths = Optional.ofNullable(yellowTaxisPath).map(v -> new String[]{v}).orElse(ConfigurationParams.YELLOW_TAXIS_INPUTS);
    log.info("Resolved file path: {}", String.join(",", paths));
    return new GenericTaxiReader<>(paths, YellowTripRecord.MAPPING, YellowTripRecord.class);
  }

  private TaxiReader<GreenTripRecord> buildGreenTaxiReader(String greenTaxisPath, DimensionsConfig config) {
    if (!config.isGreenTaxi()) {
      return new EmptyTaxiReader<>(GreenTripRecord.class);
    }
    String[] paths = Optional.ofNullable(greenTaxisPath).map(v -> new String[]{v}).orElse(ConfigurationParams.GREEN_TAXIS_INPUT);
    log.info("Resolved file path: {}", String.join(",", paths));
    return new GenericTaxiReader<>(paths, GreenTripRecord.MAPPING, GreenTripRecord.class);
  }

}
