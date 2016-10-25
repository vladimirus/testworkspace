package test.spark.arch.model;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Getter
@Builder
@ToString
@EqualsAndHashCode
public class Finance {
    @NonNull
    private Long id;
    @NonNull
    private String filename;
    @NonNull
    private LocalDate filenameDate;
    @NonNull
    private LocalDateTime loaded;
    @NonNull
    private Long noRows;
}
