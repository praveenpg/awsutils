package org.awsutils.sqs.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.function.BiFunction;

public final class Tuple2<A, B> implements ITuple {
    private final A first;
    private final B second;

    private Tuple2(final A first, final B second) {
        this.first = first;
        this.second = second;
    }

    public A _1() {
        return this.first;
    }

    public B _2() {
        return this.second;
    }

    public Tuple2<A, B> _1(final A updatedVal) {
        return new Tuple2<>(updatedVal, second);
    }

    public Tuple2<A, B> _2(final B updatedVal) {
        return new Tuple2<>(first, updatedVal);
    }


    public <X, Y> Tuple2<X, Y> map(final BiFunction<? super A, ? super B, Tuple2<X, Y>> mapper) {
        return mapper.apply(first, second);
    }

    public <C> Tuple3<A, B, C> append(final C third) {
        return Tuple3.of(first, second, third);
    }

    public Iterable<?> toIterable() {
        return Collections.unmodifiableList(Arrays.asList(first, second));
    }

    public static <A, B> Tuple2<A, B> of(final A first, final B second) {
        return new Tuple2<>(first, second);
    }

    @Override
    public String toString() {
        return "Tuple{" +
                "first=" + first +
                ", second=" + second +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Tuple2<?, ?> tuple2 = (Tuple2<?, ?>) o;
        return Objects.equals(first, tuple2.first) &&
                Objects.equals(second, tuple2.second);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }
}
