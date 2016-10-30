package test.spark.arch.printer;

import test.spark.arch.model.Result;

public class RowDiffPrinter implements Printer {
    @Override
    public String print(Result result) {
        return result.getDayOfWeek() + ","
                + result.getDaysDiff() + ","
                + round(result.getRowNoDiff()) + ","
                + result.getVerdict();
    }

    private int round(Long value) {
        return 50*(Math.round(value/50));
    }

    private String decimals(Double value) {
        return String.format("%.1f", value);
    }
}
