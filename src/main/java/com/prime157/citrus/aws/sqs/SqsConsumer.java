package com.prime157.citrus.aws.sqs;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.consol.citrus.context.TestContext;
import com.consol.citrus.endpoint.EndpointConfiguration;
import com.consol.citrus.message.DefaultMessage;
import com.consol.citrus.message.Message;
import com.consol.citrus.messaging.Consumer;

public class SqsConsumer implements Consumer {
    private final String queueUrl;
    private final AmazonSQS sqs;
    private final EndpointConfiguration endpointConfiguration;

    public SqsConsumer(final String queueUrl, final AmazonSQS sqs, final EndpointConfiguration endpointConfiguration) {
        this.queueUrl = queueUrl;
        this.sqs = sqs;
        this.endpointConfiguration = endpointConfiguration;
    }

    @Override
    public Message receive(final TestContext context) {
        return receive(context, endpointConfiguration.getTimeout());
    }

    @Override
    public Message receive(final TestContext context, final long timeout) {
        final ReceiveMessageRequest request = new ReceiveMessageRequest(queueUrl);
        request.setWaitTimeSeconds((int) TimeUnit.MILLISECONDS.toSeconds(timeout));
        final ReceiveMessageResult result = sqs.receiveMessage(request);
        if (result.getMessages().isEmpty()) {
            return null;
        }
        com.amazonaws.services.sqs.model.Message msg = result.getMessages().get(0);
        sqs.deleteMessage(queueUrl, msg.getReceiptHandle());
        final Map<String, Object> headers = new HashMap<>();
        headers.put("sqs.messageId", msg.getMessageId());
        headers.put("sqs.bodyMD5", msg.getMD5OfBody());
        headers.put("sqs.messageAttributesMD5", msg.getMD5OfMessageAttributes());
        msg.getMessageAttributes().forEach((key, value) -> headers.put("sqs." + key, value));
        return new DefaultMessage(msg.getBody(), headers);
    }

    @Override
    public String getName() {
        return "sqs-consumer-" + queueUrl;
    }

}
