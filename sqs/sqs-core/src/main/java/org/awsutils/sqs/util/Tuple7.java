package org.awsutils.sqs.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

@SuppressWarnings("unused")
public final class Tuple7<A, B, C, D, E, F, G> implements ITuple {
    private final Tuple3<A, B, C> firstThroughThird;
    private final Tuple4<D, E, F, G> fourthThroughSeventh;

    private Tuple7(final A first, final B second, final C third, final D fourth, final E fifth, final F sixth, final G seventh) {
        this.firstThroughThird = Tuple3.of(first, second, third);
        this.fourthThroughSeventh = Tuple4.of(fourth, fifth, sixth, seventh);
    }

    public A _1() {
        return this.firstThroughThird._1();
    }

    public B _2() {
        return this.firstThroughThird._2();
    }

    public C _3() {
        return this.firstThroughThird._3();
    }

    public D _4() {
        return this.fourthThroughSeventh._1();
    }

    public E _5() {
        return this.fourthThroughSeventh._2();
    }

    public F _6() {
        return this.fourthThroughSeventh._3();
    }

    public G _7() {
        return this.fourthThroughSeventh._4();
    }

    public <H> Tuple8<A, B, C, D, E, F, G, H> append(final H eigth) {
        return Tuple8.of(_1(), _2(), _3(), _4(), _5(), _6(), _7(), eigth);
    }

    public Iterable<?> toIterable() {
        return Collections.unmodifiableList(Arrays.asList(_1(), _2(), _3(), _4(), _5(), _6(), _7()));
    }

    public static <A, B, C, D, E, F, G> Tuple7<A, B, C, D, E, F, G> of(final A first, final B second, final C third, final D fourth, final E fifth, final F sixth, final G seventh) {
        return new Tuple7<>(first, second, third, fourth, fifth, sixth, seventh);
    }

    public static <A, B, C, D, E, F, G> Tuple7<A, B, C, D, E, F, G> of(Tuple6<A, B, C, D, E, F> tuple, final G seventh) {
        return new Tuple7<>(tuple._1(), tuple._2(), tuple._3(), tuple._4(), tuple._5(), tuple._6(), seventh);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Tuple7<?, ?, ?, ?, ?, ?, ?> tuple7 = (Tuple7<?, ?, ?, ?, ?, ?, ?>) o;
        return Objects.equals(firstThroughThird, tuple7.firstThroughThird) &&
                Objects.equals(fourthThroughSeventh, tuple7.fourthThroughSeventh);
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstThroughThird, fourthThroughSeventh);
    }

    @Override
    public String toString() {
        return "Tuple5{" +
                "first=" + firstThroughThird._1() +
                ", second=" + firstThroughThird._2() +
                ", third=" + firstThroughThird._3() +
                ", fourth=" + fourthThroughSeventh._1() +
                ", fifth=" + fourthThroughSeventh._2() +
                ", sixth=" + fourthThroughSeventh._3() +
                ", seventh=" + fourthThroughSeventh._4() +
                '}';
    }
}
