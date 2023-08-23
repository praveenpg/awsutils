package org.awsutils.sqs.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

@SuppressWarnings("WeakerAccess")
public final class Tuple3<A, B, C>  implements ITuple {
    private final Tuple2<A, B> firstAndSecond;
    private final C third;

    private Tuple3(final A first, final B second, final C third) {
        this.firstAndSecond = Tuple2.of(first, second);
        this.third = third;
    }

    public A _1() {
        return this.firstAndSecond._1();
    }

    public B _2() {
        return this.firstAndSecond._2();
    }

    public C _3() {
        return this.third;
    }

    public <D> Tuple4<A, B, C, D> append(final D fourth) {
        return Tuple4.of(_1(), _2(), _3(), fourth);
    }

    public Iterable<?> toIterable() {
        return Collections.unmodifiableList(Arrays.asList(_1(), _2(), _3()));
    }

    public static <A, B, C> Tuple3<A, B, C> of(final A first, final B second, final C third) {
        return new Tuple3<>(first, second, third);
    }

    public static <A, B, C> Tuple3<A, B, C> of(Tuple2<A, B> tuple2, final C third) {
        return new Tuple3<>(tuple2._1(), tuple2._2(), third);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Tuple3<?, ?, ?> tuple3 = (Tuple3<?, ?, ?>) o;
        return Objects.equals(firstAndSecond, tuple3.firstAndSecond) &&
                Objects.equals(third, tuple3.third);
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstAndSecond, third);
    }

    @Override
    public String toString() {
        return "Tuple3{" +
                "first=" + firstAndSecond._1() +
                ", second=" + firstAndSecond._2() +
                ", third=" + third +
                '}';
    }
}
