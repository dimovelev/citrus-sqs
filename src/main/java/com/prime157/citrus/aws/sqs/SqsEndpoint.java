package com.prime157.citrus.aws.sqs;

import com.amazonaws.services.sqs.AmazonSQS;
import com.consol.citrus.endpoint.AbstractEndpoint;
import com.consol.citrus.endpoint.EndpointConfiguration;
import com.consol.citrus.messaging.Consumer;
import com.consol.citrus.messaging.Producer;

public class SqsEndpoint extends AbstractEndpoint {
    private final String queueUrl;
    private final AmazonSQS sqs;

    public SqsEndpoint(final EndpointConfiguration endpointConfiguration, final String queueUrl, final AmazonSQS sqs) {
        super(endpointConfiguration);
        this.queueUrl = queueUrl;
        this.sqs = sqs;
    }

    @Override
    public Producer createProducer() {
        return new SqsProducer(queueUrl, sqs);
    }

    @Override
    public Consumer createConsumer() {
        return new SqsConsumer(queueUrl, sqs, getEndpointConfiguration());
    }
}
