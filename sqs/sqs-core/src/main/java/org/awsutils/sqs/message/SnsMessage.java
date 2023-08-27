package org.awsutils.sqs.message;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.awsutils.sqs.client.SnsService;
import org.awsutils.common.exceptions.UtilsException;
import org.awsutils.common.util.ApplicationContextUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.annotation.Qualifier;
import software.amazon.awssdk.services.sns.model.PublishResponse;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

@Configurable
@JsonIgnoreProperties
public class SnsMessage<T> extends AbstractAwsMessage<T> {
    @Autowired
    @Qualifier("snsService")
    private SnsService snsService;

    public SnsMessage(final String messageType, final T message) {
        super(messageType, message);
    }

    public SnsMessage(final String messageType, final T message, final Function<Throwable, RuntimeException> exceptionFunc) {
        super(messageType, message, exceptionFunc);
    }

    public SnsMessage(final String transactionId, final String messageType, final T message) {
        super(transactionId, messageType, message);
    }

    public SnsMessage(final String transactionId, final String messageType, final T message, final Function<Throwable, RuntimeException> exceptionFunc) {
        super(transactionId, messageType, message, exceptionFunc);
    }

    public CompletableFuture<PublishResponse> publish(final String topicArn) {

        return publish(topicArn, Collections.emptyMap());
    }

    public CompletableFuture<PublishResponse> publish(final String topicArn, final Map<String, String> attributes) {
        final SnsService snsService = this.snsService != null ? this.snsService : ApplicationContextUtils.getInstance().getBean(SnsService.class, "snsService");

        return snsService.publishMessage(this, topicArn, attributes);
    }

    public interface Builder<T> {
        Builder<T> transactionId(String transactionId);
        Builder<T> messageType(String messageType);
        Builder<T> message(T message);
        Builder<T> exceptionFunc(Function<Throwable, RuntimeException> exceptionFunc);
        SnsMessage<T> build();
    }

    private static class BuilderImpl<T> implements Builder<T> {
        private String transactionId;
        private String messageType;
        private T message;
        private Function<Throwable, RuntimeException> exceptionFunc = cause -> new UtilsException("UNKNOWN_ERROR", cause);

        @Override
        public Builder<T> transactionId(final String transactionId) {
            this.transactionId = transactionId;
            return this;
        }

        @Override
        public Builder<T> messageType(final String messageType) {
            this.messageType = messageType;
            return this;
        }

        @Override
        public Builder<T> message(final T message) {
            this.message = message;
            return this;
        }

        @Override
        public Builder<T> exceptionFunc(final Function<Throwable, RuntimeException> exceptionFunc) {
            this.exceptionFunc = exceptionFunc;
            return this;
        }

        @Override
        public SnsMessage<T> build() {
            return new SnsMessage<>(transactionId, messageType, message, exceptionFunc);
        }
    }

    @Override
    public String toString() {
        return "SnsMessage{} " + super.toString();
    }
}
