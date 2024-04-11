package ai.cognitiv.nyc.taxi.reader.cognitiv.config;

import java.time.LocalDateTime;
import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class DimensionsConfig {

  LocalDateTime start;
  LocalDateTime end;
  Long pickUpLocationId;
  Long dropOffLocationId;
  Long vendorId;
  boolean greenTaxi;
  boolean yellowTaxi;

}
