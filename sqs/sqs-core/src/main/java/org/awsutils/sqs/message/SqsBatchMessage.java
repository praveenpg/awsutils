package org.awsutils.sqs.message;

import java.util.List;
import java.util.Objects;


public final class SqsBatchMessage<T> {
    private final List<SqsMessage<T>> sqsMessages;

    public SqsBatchMessage(List<SqsMessage<T>> sqsMessages) {
        this.sqsMessages = sqsMessages;
    }

    public List<SqsMessage<T>> sqsMessages() {
        return sqsMessages;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (SqsBatchMessage) obj;
        return Objects.equals(this.sqsMessages, that.sqsMessages);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sqsMessages);
    }

    @Override
    public String toString() {
        return "SqsBatchMessage[" +
                "sqsMessages=" + sqsMessages + ']';
    }

}
