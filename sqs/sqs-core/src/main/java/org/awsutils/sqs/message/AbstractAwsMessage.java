package org.awsutils.sqs.message;

import org.awsutils.sqs.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

@SuppressWarnings("unchecked")
public abstract class AbstractAwsMessage<T> implements AwsMessage {
    private final String transactionId;
    private final String messageType;
    private final Map<String, ?> message;
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAwsMessage.class);
    private static Class<? extends Enum<?>> messageEnumType;

    AbstractAwsMessage(final String messageType, final T message) {
        this(messageType, message, RuntimeException::new);
    }

    AbstractAwsMessage(final String messageType, final T message, final Function<Throwable, RuntimeException> exceptionFunc) {
        this(UUID.randomUUID().toString(), messageType, message, exceptionFunc);
    }

    AbstractAwsMessage(final String transactionId, final String messageType, final T message) {
        this(transactionId, messageType, message, RuntimeException::new);
    }

    AbstractAwsMessage(final String transactionId, final String messageType, final T message, final Function<Throwable, RuntimeException> exceptionFunc) {
        this.transactionId = transactionId;
        this.messageType = messageType;
        this.message = Utils.constructFromJson(HashMap.class, Utils.constructJson(message), exceptionFunc);

        if(messageEnumType != null) {
            validateMessageType();
        }
    }

    @Override
    public String getMessageType() {
        return messageType;
    }

    @SuppressWarnings("unused")

    @Override
    public Map<String, ?> getMessage() {
        return message;
    }


    @Override
    public String getTransactionId() {
        return transactionId;
    }


    public static void messageEnumType(final Class<? extends Enum<?>> validationEnum) {
        AbstractAwsMessage.messageEnumType = validationEnum;
    }

    private void validateMessageType() {
        try {
            LOGGER.debug("Sqs Message Type: " + Enum.valueOf((Class) messageEnumType, messageType));
        } catch (final IllegalArgumentException e) {
            LOGGER.error(MessageFormat.format("The messageType [{0}] does not match any constant defined in the Enum [{1}]", this.messageType, messageEnumType.getSimpleName()), e);

            throw e;
        }
    }

    @Override
    public String toString() {
        return "AbstractAwsMessage{" +
                "transactionId='" + transactionId + '\'' +
                ", messageType='" + messageType + '\'' +
                ", message=" + message +
                '}';
    }

    public interface Builder<T, A extends AbstractAwsMessage<T>> {
        <X extends Builder<T, A>> X transactionId(String transactionId);
        <X extends Builder<T, A>> X messageType(String messageType);

        <X extends Builder<T, A>> X message(T message);

        <X extends Builder<T, A>> X exceptionFunc(Function<Throwable, RuntimeException> exceptionFunc);

        A build();
    }

    static abstract class BuilderImpl<T, A extends AbstractAwsMessage<T>> implements Builder<T, A> {
        protected String transactionId;
        protected String messageType;
        protected T message;

        protected Function<Throwable, RuntimeException> exceptionFunc;

        @Override
        public <X extends Builder<T, A>> X transactionId(String transactionId) {
            this.transactionId = transactionId;
            return (X) this;
        }

        @Override
        public <X extends Builder<T, A>> X messageType(String messageType) {
            this.messageType = messageType;
            return (X) this;
        }

        @Override
        public <X extends Builder<T, A>> X message(T message) {
            this.message = message;
            return (X) this;
        }

        public <X extends Builder<T, A>> X exceptionFunc(Function<Throwable, RuntimeException> exceptionFunc) {
            this.exceptionFunc = exceptionFunc;

            return (X) this;
        }
    }
}
