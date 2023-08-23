package org.awsutils.sqs.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

@SuppressWarnings("WeakerAccess")
public final class Tuple4<A, B, C, D> implements ITuple {
    private final Tuple2<A, B> firstAndSecond;
    private final Tuple2<C, D> thirdAndFourth;

    private Tuple4(final A first, final B second, final C third, final D fourth) {
        this.firstAndSecond = Tuple2.of(first, second);
        this.thirdAndFourth = Tuple2.of(third, fourth);
    }

    public A _1() {
        return this.firstAndSecond._1();
    }

    public B _2() {
        return this.firstAndSecond._2();
    }

    public C _3() {
        return this.thirdAndFourth._1();
    }

    public D _4() {
        return this.thirdAndFourth._2();
    }

    public <E> Tuple5<A, B, C, D, E> append(final E fifth) {
        return Tuple5.of(_1(), _2(), _3(), _4(), fifth);
    }

    public Iterable<?> toIterable() {
        return Collections.unmodifiableList(Arrays.asList(_1(), _2(), _3(), _4()));
    }

    public static <A, B, C, D> Tuple4<A, B, C, D> of(final A first, final B second, final C third, final D fourth) {
        return new Tuple4<>(first, second, third, fourth);
    }

    public static <A, B, C, D> Tuple4<A, B, C, D> of(Tuple3<A, B, C> tuple, final D fourth) {
        return new Tuple4<>(tuple._1(), tuple._2(), tuple._3(), fourth);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Tuple4<?, ?, ?, ?> tuple4 = (Tuple4<?, ?, ?, ?>) o;
        return Objects.equals(firstAndSecond, tuple4.firstAndSecond) &&
                Objects.equals(thirdAndFourth, tuple4.thirdAndFourth);
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstAndSecond, thirdAndFourth);
    }

    @Override
    public String toString() {
        return "Tuple4{" +
                "first=" + firstAndSecond._1() +
                ", second=" + firstAndSecond._2() +
                ", third=" + thirdAndFourth._1() +
                ", fourth=" + thirdAndFourth._2() +
                '}';
    }
}
