package ai.cognitiv.nyc.taxi.reader.cognitiv.config;

import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class ConfigurationParams {

  static final String[] YELLOW_TAXIS_INPUTS = new String[]{
      "src/main/resources/data/nyc/taxi/11/yellow_tripdata_2023-11.parquet",
      "src/main/resources/data/nyc/taxi/10/yellow_tripdata_2023-10.parquet"
  };

  static final String[] GREEN_TAXIS_INPUT = new String[]{
      "src/main/resources/data/nyc/taxi/11/green_tripdata_2023-11.parquet",
      "src/main/resources/data/nyc/taxi/10/green_tripdata_2023-10.parquet"
  };

  String yellowTaxisPath;
  String greenTaxisPath;
  DimensionsConfig dimensionsConfig;
  ReportingConfig reportingConfig;



}
