package test.spark.arch.model;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Builder
@Getter
@ToString
public class Result<T> {
    private Integer dayOfWeek;
    private Long rowNoDiff;
    private Long daysDiff;
    private T underlying;
    private Integer verdict;

    public String print() {
        return dayOfWeek + "," + round(rowNoDiff) + "," + daysDiff + "," + verdict;
    }

    private Long round(Long num) {
        return ((num + 99) / 100 );
    }
}
