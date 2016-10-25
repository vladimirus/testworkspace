package test.spark.arch;

import static java.util.stream.Collectors.toList;

import lombok.SneakyThrows;
import test.spark.arch.model.Finance;
import test.spark.arch.model.FinanceFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;

public class Transform {
    private FinanceFactory financeFactory;
    private Path file;

    public Transform(FinanceFactory financeFactory, Path file) {
        this.financeFactory = financeFactory;
        this.file = file;
    }

    @SneakyThrows
    public Collection<Finance> lines() {
        return Files.lines(file)
                .skip(1)
                .map(line -> financeFactory.finance(line))
                .sorted((a,b) -> a.getId().compareTo(b.getId()))
                .collect(toList());
    }
}
