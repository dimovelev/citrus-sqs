package com.prime157.citrus.aws.sqs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils;

import com.consol.citrus.context.TestContext;
import com.consol.citrus.endpoint.EndpointConfiguration;
import com.consol.citrus.message.RawMessage;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

class SqsEndpointTest {
    private static LocalStackContainer localstack;
    private static SqsClient sqs;
    private String queueUrl;
    private TestContext context;

    private SqsEndpoint testee;

    @BeforeAll
    static void beforeAll() throws URISyntaxException {
        localstack = new LocalStackContainer() //
                .withServices(SQS) //
                .withEnv("DEFAULT_REGION", "us-east-1") //
                .withEnv("HOSTNAME_EXTERNAL", "localhost") //
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("localstack")));

        localstack.start();

        final URI uri =
                new URI("http://" + localstack.getContainerIpAddress() + ":" + localstack.getMappedPort(SQS.getPort()));
        final AwsCredentials awsCredentials = AwsBasicCredentials.create("accesskey", "secretkey");
        final ProxyConfiguration proxy =
                ProxyConfiguration.builder().nonProxyHosts(new HashSet<>(Collections.singletonList("*"))).build();
        final ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder() //
                .proxyConfiguration(proxy) //
                .connectionTimeout(Duration.ofSeconds(1)) //
                .socketTimeout(Duration.ofSeconds(10));
        sqs = SqsClient.builder() //
                .endpointOverride(uri) //
                .credentialsProvider(StaticCredentialsProvider.create(awsCredentials)) //
                .httpClientBuilder(httpClientBuilder) //
                .region(Region.US_EAST_1) //
                .build();
    }

    @BeforeEach
    void beforeEach() {
        final CreateQueueRequest req = CreateQueueRequest.builder().queueName(UUID.randomUUID().toString()).build();
        final CreateQueueResponse response = sqs.createQueue(req);
        queueUrl = response.queueUrl();
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
    public static void afterAll() {
        localstack.close();
    }

    @Test
    void produce() {
        final String payload = RandomStringUtils.randomAlphanumeric(20);

        testee.createProducer().send(new RawMessage(payload), context);

        assertThat(context.getVariables()).containsKeys("sqs.sendMessageResponse").isNotNull();
        final ReceiveMessageResponse result =
                sqs.receiveMessage(ReceiveMessageRequest.builder().queueUrl(queueUrl).build());
        assertThat(result.messages()).hasSize(1);
        final Message msg = result.messages().get(0);
        assertThat(msg.body()).isEqualTo(payload);
    }

    @Test
    void consume() {
        final String payload = RandomStringUtils.randomAlphanumeric(20);
        final SendMessageRequest req = SendMessageRequest.builder().queueUrl(queueUrl).messageBody(payload).build();
        final SendMessageResponse sms = sqs.sendMessage(req);

        com.consol.citrus.message.Message result = testee.createConsumer().receive(context);

        assertThat(result).isNotNull();
        assertThat(result.getPayload()).isEqualTo(payload);
        assertThat(result.getHeader("sqs.messageId")).isEqualTo(sms.messageId());
    }
}
