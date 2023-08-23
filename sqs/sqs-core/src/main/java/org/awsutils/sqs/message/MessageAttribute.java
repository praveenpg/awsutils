package org.awsutils.sqs.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class MessageAttribute {
    @JsonProperty("Type")
    private final String type;

    @JsonProperty("Value")
    private final String value;

    @JsonCreator
    public MessageAttribute(@JsonProperty("Type") final String type, @JsonProperty("Value") final String value) {
        this.type = type;
        this.value = value;
    }

    public String getType() {
        return type;
    }

    public String getValue() {
        return value;
    }
}
