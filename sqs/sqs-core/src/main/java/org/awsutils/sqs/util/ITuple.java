package org.awsutils.sqs.util;

import java.io.Serializable;

public interface ITuple extends Serializable {
    default <A> ITuple append(final A a) {
        throw new UnsupportedOperationException();
    }

    Iterable<?> toIterable();
}
