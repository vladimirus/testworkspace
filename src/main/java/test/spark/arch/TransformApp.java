package test.spark.arch;

import static java.lang.Math.abs;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static test.spark.Utils.getResourceUrl;

import org.apache.commons.lang3.tuple.Pair;
import test.spark.arch.model.Finance;
import test.spark.arch.model.FinanceFactory;
import test.spark.arch.model.Previous;
import test.spark.arch.model.Result;

import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.stream.Stream;

public class TransformApp {


    public static void main(String[] args) {
        TransformApp app = new TransformApp();
        Transform transform = new Transform(new FinanceFactory(), Paths.get(getResourceUrl("arch/Arch_Finance.txt")));

        // transform
        Collection<Result> results = app.results(transform.lines());

        // lines
        Collection<String> lines = app.lines(results);

        // print
        lines.forEach(l -> System.out.println(l));

    }

    Collection<Result> results(Collection<Finance> lines) {
        Previous<Finance> previous = new Previous<>();
        return lines.stream()
                .map(l -> {
                    Result result = Result.builder()
                            .age(DAYS.between(LocalDate.parse("2014-07-07"), l.getLoaded()))
                            .dayOfWeek(l.getFilenameDate().getDayOfWeek().getValue())
                            .rows(l.getNoRows())
                            .rowNoDiff(l.getNoRows() - previous.get().map(Finance::getNoRows).orElse(l.getNoRows()))
                            .daysDiff(DAYS.between(previous.get().map(Finance::getFilenameDate).orElse(l.getFilenameDate()), l.getFilenameDate()))
                            .underlying(l)
                            .build();
                    previous.set(l);
                    return result;
                })
                .filter(p -> p.getDaysDiff() != 0)
                .collect(toList());
    }

    Collection<String> lines(Collection<Result> results) {
         return results.stream()
                .filter(r -> !(r.getRowNoDiff() > 5000 || r.getRowNoDiff() < -3000))
                .flatMap(r -> {
                    Pair<Long, Long> rowDiff = boundaries(r.getRowNoDiff());
                    Pair<Long, Long> rows = boundaries(r.getRows());
                    Long age = DAYS.between(LocalDate.parse("2014-07-07"), ((Finance)r.getUnderlying()).getLoaded());

                    return Stream.of(
                            Result.builder().age(age).rows(rows.getLeft()).dayOfWeek(r.getDayOfWeek()).daysDiff(r.getDaysDiff()).rowNoDiff(rowDiff.getLeft()).verdict(0).build(),
                            Result.builder().age(age).rows(r.getRows()).dayOfWeek(r.getDayOfWeek()).daysDiff(r.getDaysDiff()).rowNoDiff(r.getRowNoDiff()).verdict(1).build(),
                            Result.builder().age(age).rows(rows.getRight()).dayOfWeek(r.getDayOfWeek()).daysDiff(r.getDaysDiff()).rowNoDiff(rowDiff.getRight()).verdict(0).build()
                    );
                })
                .map(Result::print)
                .collect(toList());

    }

    private Pair<Long, Long> boundaries(Long real) {
        Long upper = (abs(real) / 2) + abs(real);
        Long lower =  abs(real) - (abs(real) / 2);
        return Pair.of(upper, lower);
    }
}
