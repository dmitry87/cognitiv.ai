package ai.cognitiv.nyc.taxi.reader.cognitiv.model;

public enum TaxiColor {
  GREEN("green"),
  YELLOW("yellow");

  TaxiColor(String yellow) {
    this.color = yellow;
  }

  private String color;

  public String getColor() {
    return color;
  }
}
