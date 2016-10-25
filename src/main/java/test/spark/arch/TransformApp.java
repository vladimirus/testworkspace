package test.spark.arch;

import static java.lang.Math.abs;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static test.spark.Utils.getResourceUrl;

import test.spark.arch.model.Finance;
import test.spark.arch.model.FinanceFactory;
import test.spark.arch.model.Previous;
import test.spark.arch.model.Result;

import java.nio.file.Paths;
import java.time.temporal.ChronoUnit;
import java.util.Collection;

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
                            .dayOfWeek(l.getFilenameDate().getDayOfWeek().getValue())
                            .rowNoDiff(l.getNoRows() - previous.get().map(Finance::getNoRows).orElse(l.getNoRows()))
                            .daysDiff(ChronoUnit.DAYS.between(previous.get().map(Finance::getFilenameDate).orElse(l.getFilenameDate()), l.getFilenameDate()))
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
                    Long upper = (abs(r.getRowNoDiff()) / 2) + abs(r.getRowNoDiff());
                    Long lower =  abs(r.getRowNoDiff()) - (abs(r.getRowNoDiff()) / 2);
                    if (r.getRowNoDiff() < 0) {
                        upper *= -1;
                        lower *= -1;
                    }

                    return asList(
                            Result.builder().dayOfWeek(r.getDayOfWeek()).daysDiff(r.getDaysDiff()).rowNoDiff(lower).verdict(0).build(),
                            Result.builder().dayOfWeek(r.getDayOfWeek()).daysDiff(r.getDaysDiff()).rowNoDiff(r.getRowNoDiff()).verdict(1).build(),
                            Result.builder().dayOfWeek(r.getDayOfWeek()).daysDiff(r.getDaysDiff()).rowNoDiff(upper).verdict(0).build()
                    ).stream();
                })
                .map(Result::print)
                .collect(toList());

    }
}
