package org.awsutils.sqs.message;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "Type",
        "MessageId",
        "TopicArn",
        "Message",
        "Timestamp",
        "SignatureVersion",
        "Signature",
        "SigningCertURL",
        "UnsubscribeURL"
})
public class SnsSubscriptionMessage {

    @JsonProperty("Type")
    private String type;
    @JsonProperty("MessageId")
    private String messageId;
    @JsonProperty("TopicArn")
    private String topicArn;
    @JsonProperty("Message")
    private String message;
    @JsonProperty("Timestamp")
    private String timestamp;
    @JsonProperty("SignatureVersion")
    private String signatureVersion;
    @JsonProperty("Signature")
    private String signature;
    @JsonProperty("SigningCertURL")
    private String signingCertURL;
    @JsonProperty("UnsubscribeURL")
    private String unsubscribeURL;
    @JsonProperty("MessageAttributes")
    private Map<String, MessageAttribute> messageAttributes;

    @JsonProperty("Type")
    public String getType() {
        return type;
    }

    @JsonProperty("Type")
    public void setType(String type) {
        this.type = type;
    }

    @JsonProperty("MessageId")
    public String getMessageId() {
        return messageId;
    }

    @JsonProperty("MessageId")
    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    @JsonProperty("TopicArn")
    public String getTopicArn() {
        return topicArn;
    }

    @JsonProperty("TopicArn")
    public void setTopicArn(String topicArn) {
        this.topicArn = topicArn;
    }

    @JsonProperty("Message")
    public String getMessage() {
        return message;
    }

    @JsonProperty("Message")
    public void setMessage(String message) {
        this.message = message;
    }

    @JsonProperty("Timestamp")
    public String getTimestamp() {
        return timestamp;
    }

    @JsonProperty("Timestamp")
    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    @JsonProperty("SignatureVersion")
    public String getSignatureVersion() {
        return signatureVersion;
    }

    @JsonProperty("SignatureVersion")
    public void setSignatureVersion(String signatureVersion) {
        this.signatureVersion = signatureVersion;
    }

    @JsonProperty("Signature")
    public String getSignature() {
        return signature;
    }

    @JsonProperty("Signature")
    public void setSignature(String signature) {
        this.signature = signature;
    }

    @JsonProperty("SigningCertURL")
    public String getSigningCertURL() {
        return signingCertURL;
    }

    @JsonProperty("SigningCertURL")
    public void setSigningCertURL(String signingCertURL) {
        this.signingCertURL = signingCertURL;
    }

    @JsonProperty("UnsubscribeURL")
    public String getUnsubscribeURL() {
        return unsubscribeURL;
    }

    @JsonProperty("UnsubscribeURL")
    public void setUnsubscribeURL(String unsubscribeURL) {
        this.unsubscribeURL = unsubscribeURL;
    }

    @JsonProperty("MessageAttributes")
    public Map<String, MessageAttribute> getMessageAttributes() {
        return messageAttributes;
    }

    @JsonProperty("MessageAttributes")
    public void setMessageAttributes(final Map<String, MessageAttribute> messageAttributes) {
        this.messageAttributes = messageAttributes;
    }
}