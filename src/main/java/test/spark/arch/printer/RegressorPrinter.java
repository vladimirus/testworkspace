package test.spark.arch.printer;

import test.spark.arch.model.Result;

public class RegressorPrinter implements Printer {
    @Override
    public String print(Result result) {
        return result.getAge() + ","
                + result.getDayOfWeek() + ","
                + result.getDaysDiff() + ","
                + twoDecimals(round(result.getRows()));
    }

    private Double round(Long value) {
        return value / 1000000D;
    }

    private String twoDecimals(Double value) {
        return String.format("%.2f", value);
    }
}
