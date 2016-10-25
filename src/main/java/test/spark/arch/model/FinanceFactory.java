package test.spark.arch.model;

import static com.google.common.base.Splitter.on;
import static java.time.LocalDate.parse;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class FinanceFactory {
    private DateTimeFormatter DATE_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
    private DateTimeFormatter FILENAME_DATE = DateTimeFormatter.ofPattern("yyMMdd");

    public Finance finance(String line) {
        List<String> list = list(",", line);
        return Finance.builder()
                .id(Long.valueOf(list.get(0)))
                .filename(list.get(1))
                .loaded(LocalDateTime.parse(list.get(2), DATE_TIME))
                .noRows(Long.valueOf(list.get(3)))
                .filenameDate(filenameDate(list.get(1)))
                .build();
    }

    private LocalDate filenameDate(String str) {
        return parse(list("_", str).get(2).replaceAll("\\..*", ""), FILENAME_DATE);
    }

    private List<String> list(String separator, String line) {
        return on(separator)
                .omitEmptyStrings()
                .trimResults()
                .splitToList(line);
    }
}
