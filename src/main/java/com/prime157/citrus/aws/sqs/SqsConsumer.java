package com.prime157.citrus.aws.sqs;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.consol.citrus.context.TestContext;
import com.consol.citrus.endpoint.EndpointConfiguration;
import com.consol.citrus.message.DefaultMessage;
import com.consol.citrus.message.Message;
import com.consol.citrus.messaging.Consumer;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

public class SqsConsumer implements Consumer {
    private final String queueUrl;
    private final SqsClient sqs;
    private final EndpointConfiguration endpointConfiguration;

    public SqsConsumer(final String queueUrl, final SqsClient sqs, final EndpointConfiguration endpointConfiguration) {
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
        final ReceiveMessageRequest request = ReceiveMessageRequest.builder() //
                .queueUrl(queueUrl) //
                .waitTimeSeconds((int) TimeUnit.MILLISECONDS.toSeconds(timeout)) //
                .build();
        final ReceiveMessageResponse result = sqs.receiveMessage(request);
        if (!result.hasMessages()) {
            return null;
        }
        final software.amazon.awssdk.services.sqs.model.Message msg = result.messages().get(0);
        sqs.deleteMessage(DeleteMessageRequest.builder() //
                .queueUrl(queueUrl) //
                .receiptHandle(msg.receiptHandle()) //
                .build() //
        );
        final Map<String, Object> headers = new HashMap<>();
        headers.put("sqs.messageId", msg.messageId());
        headers.put("sqs.bodyMD5", msg.md5OfBody());
        headers.put("sqs.messageAttributesMD5", msg.md5OfMessageAttributes());
        msg.messageAttributes().forEach((key, value) -> headers.put("sqs." + key, value));
        return new DefaultMessage(msg.body(), headers);
    }

    @Override
    public String getName() {
        return "sqs-consumer-" + queueUrl;
    }

}
