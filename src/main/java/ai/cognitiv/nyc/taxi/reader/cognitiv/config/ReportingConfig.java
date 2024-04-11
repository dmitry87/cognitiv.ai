package ai.cognitiv.nyc.taxi.reader.cognitiv.config;

import ai.cognitiv.nyc.taxi.reader.cognitiv.model.TaxiColor;
import java.time.LocalDateTime;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class ReportingConfig {

  LocalDateTime start;
  LocalDateTime end;
  Long pickUpLocationId;
  Long dropOffLocationId;
  Long vendorId;
  TaxiColor taxiColor;


}
