package com.prime157.citrus.aws.sqs;

import com.consol.citrus.context.TestContext;
import com.consol.citrus.message.Message;
import com.consol.citrus.messaging.Producer;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

public class SqsProducer implements Producer {
    private final String queueUrl;
    private final SqsClient sqs;

    public SqsProducer(final String queueUrl, final SqsClient sqs) {
        this.queueUrl = queueUrl;
        this.sqs = sqs;
    }

    @Override
    public void send(final Message message, final TestContext context) {
        final SendMessageRequest request = SendMessageRequest.builder() //
                .queueUrl(queueUrl) //
                .messageBody(message.getPayload(String.class)) //
                .build();
        final SendMessageResponse sendMessageResponse = sqs.sendMessage(request);
        context.setVariable("sqs.sendMessageResponse", sendMessageResponse);
    }

    @Override
    public String getName() {
        return "sqs-producer-" + queueUrl;
    }
}
