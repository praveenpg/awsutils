package org.awsutils.sqs.message;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TaskInput<T> {
    @JsonProperty("Input")
    private SqsMessage<T> input;

    @JsonProperty("TaskToken")
    private String taskToken;

    public SqsMessage<T> getInput() {
        return input;
    }

    public void setInput(final SqsMessage<T> input) {
        this.input = input;
    }

    public String getTaskToken() {
        return taskToken;
    }

    public void setTaskToken(final String taskToken) {
        this.taskToken = taskToken;
    }
}
