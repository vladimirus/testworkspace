package test.spark.arch.model;

import static java.util.Optional.ofNullable;

import java.util.Optional;

public class Previous<T> {
    private T previous;

    public Optional<T> get() {
        return ofNullable(previous);
    }

    public void set(T previous) {
        this.previous = previous;
    }
}
