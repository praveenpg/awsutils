package org.awsutils.sqs.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.springframework.beans.factory.annotation.Configurable;

import java.util.function.Function;

@Configurable
@JsonIgnoreProperties
public class SqsMessage<T> extends AbstractAwsMessage<T> {
    SqsMessage(String messageType, T message) {
        super(messageType, message);
    }

    SqsMessage(String messageType, T message, Function<Throwable, RuntimeException> exceptionFunc) {
        super(messageType, message, exceptionFunc);
    }

    @JsonCreator
    SqsMessage(@JsonProperty("transactionId") String transactionId, @JsonProperty("messageType") String messageType,  @JsonProperty("message") T message) {
        super(transactionId, messageType, message);
    }

    SqsMessage(String transactionId, String messageType, T message, Function<Throwable, RuntimeException> exceptionFunc) {
        super(transactionId, messageType, message, exceptionFunc);
    }

    public static <A> Builder<A, SqsMessage<A>> builder() {
        return new SqsMessageBuilder<>();
    }

    private static class SqsMessageBuilder<A> extends AbstractAwsMessage.BuilderImpl<A, SqsMessage<A>> {

        @Override
        public SqsMessage<A> build() {
            return new SqsMessage<>(super.transactionId, super.messageType, message, super.exceptionFunc);
        }
    }
}
