package com.prime157.citrus.aws.sqs;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.consol.citrus.context.TestContext;
import com.consol.citrus.message.Message;
import com.consol.citrus.messaging.Producer;

public class SqsProducer implements Producer {
    private final String queueUrl;
    private final AmazonSQS sqs;

    public SqsProducer(final String queueUrl, final AmazonSQS sqs) {
        this.queueUrl = queueUrl;
        this.sqs = sqs;
    }

    @Override
    public void send(final Message message, final TestContext context) {
        final SendMessageResult sendMessageResult = sqs.sendMessage(queueUrl, message.getPayload(String.class));
        context.setVariable("sqs.sendMessageResult", sendMessageResult);
    }

    @Override
    public String getName() {
        return "sqs-producer-" + queueUrl;
    }
}
