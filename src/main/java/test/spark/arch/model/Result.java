package test.spark.arch.model;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Builder
@Getter
@ToString
public class Result<T> {
    private Long age;
    private Integer dayOfWeek;
    private Long rows;
    private Long rowNoDiff;
    private Long daysDiff;
    private T underlying;
    private Integer verdict;

    public String print() {
        return age + "," + dayOfWeek + "," + round(rows) + "," + daysDiff + "," + verdict;
    }

    private Double round(Long num) {
        return num / 1000000D;
//        return ((num + 99) / 100 );
    }
}
