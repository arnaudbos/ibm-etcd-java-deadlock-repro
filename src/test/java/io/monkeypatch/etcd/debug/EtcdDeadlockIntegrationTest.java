package io.monkeypatch.etcd.debug;

import com.google.common.io.Resources;
import com.ibm.etcd.client.EtcdClient;
import com.palantir.docker.compose.DockerComposeRule;
import io.grpc.Deadline;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.net.URISyntaxException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.ibm.etcd.client.KeyUtils.bs;

public class EtcdDeadlockIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(EtcdDeadlockIntegrationTest.class);
    private static final long DEFAULT_TIMEOUT_MS = 1L;
    private static final long DEADLINE_TIMEOUT_MS = 1L;

    private static String etcdHost1;

    private static String dockerComposePath;
    static {
        try {
            dockerComposePath = new java.io.File(Resources.getResource("docker-compose.yml").toURI()).getAbsolutePath();
        } catch (URISyntaxException e) { e.printStackTrace(); }
    }

    private static DockerComposeRule docker;
    private static EtcdClient client;

    @ClassRule
    public static TestRule chain = RuleChain
            .outerRule(docker = DockerComposeRule.builder()
                    .file(dockerComposePath)
                    .pullOnStartup(true)
                    .build())
            .around(new ExternalResource() {
                @Override
                protected void before() {
                    etcdHost1 = docker.containers()
                            .container("etcd")
                            .port(2379)
                            .inFormat("http://$HOST:$EXTERNAL_PORT");
                }
            });

    @BeforeClass
    public static void setup() {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        client = EtcdClient.forEndpoints(etcdHost1)
                .withDefaultTimeout(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .withPlainText()
                .build();
    }

    @Test(timeout = 20_000L)
    public void testEtcdClient() {
        // Repetitively try to request etcd with very short timeout and deadline
        // We should get "deadline exceeded" errors quickly but most of times we deadlock
        IntStream.range(0, 10).forEach(i -> {
            try {
                LOG.error("Trying to put random key");
                client.getKvClient()
                        .put(bs(UUID.randomUUID().toString()), bs("some-value"))
                        .timeout(DEFAULT_TIMEOUT_MS)
                        .backoffRetry()
                        .deadline(Deadline.after(DEADLINE_TIMEOUT_MS, TimeUnit.MILLISECONDS))
                        .sync();
            } catch (Exception e) {
                LOG.error("Put should have failed with timeout, retrying...", e);
            }
        });
    }
}