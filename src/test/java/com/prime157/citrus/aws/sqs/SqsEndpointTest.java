package com.prime157.citrus.aws.sqs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

import java.util.UUID;

import org.junit.AfterClass;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.consol.citrus.context.TestContext;
import com.consol.citrus.endpoint.EndpointConfiguration;
import com.consol.citrus.message.RawMessage;

class SqsEndpointTest {
    private static LocalStackContainer localstack;
    private static AmazonSQS sqs;
    private String queueUrl;
    private TestContext context;

    private SqsEndpoint testee;

    @BeforeAll
    static void beforeAll() {
        localstack = new LocalStackContainer() //
                .withServices(SQS) //
                .withEnv("DEFAULT_REGION", "us-east-1") //
                .withEnv("HOSTNAME_EXTERNAL", "localhost");
        localstack.start();

        sqs = AmazonSQSClientBuilder //
                .standard() //
                .withEndpointConfiguration(localstack.getEndpointConfiguration(SQS)) //
                .withCredentials(localstack.getDefaultCredentialsProvider()) //
                .withClientConfiguration(new ClientConfiguration() //
                        .withNonProxyHosts("*") //
                ) //
                .build();
    }

    @BeforeEach
    void beforeEach() {
        final CreateQueueResult result = sqs.createQueue(UUID.randomUUID().toString());
        queueUrl = result.getQueueUrl();
        context = new TestContext();
        testee = new SqsEndpoint(new EndpointConfiguration() {
            @Override
            public long getTimeout() {
                return 2_000;
            }

            @Override
            public void setTimeout(final long timeout) {

            }
        }, queueUrl, sqs);
    }

    @AfterClass
    static void afterAll() {
        localstack.close();
    }

    @Test
    void produce() {
        final String payload = RandomStringUtils.randomAlphanumeric(20);

        testee.createProducer().send(new RawMessage(payload), context);

        assertThat(context.getVariables()).containsKeys("sqs.sendMessageResult").isNotNull();
        final ReceiveMessageResult result = sqs.receiveMessage(queueUrl);
        assertThat(result.getMessages()).hasSize(1);
        final Message msg = result.getMessages().get(0);
        assertThat(msg.getBody()).isEqualTo(payload);
    }

    @Test
    void consume() {
        final String payload = RandomStringUtils.randomAlphanumeric(20);
        final SendMessageResult sms = sqs.sendMessage(queueUrl, payload);

        com.consol.citrus.message.Message result = testee.createConsumer().receive(context);
        assertThat(result).isNotNull();
        assertThat(result.getPayload()).isEqualTo(payload);
        assertThat(result.getHeader("sqs.messageId")).isEqualTo(sms.getMessageId());
    }
}
