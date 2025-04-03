package io.aiven.kafka.connect.azure.source;

import io.aiven.kafka.connect.azure.source.testdata.AzureIntegrationTestData;
import io.aiven.kafka.connect.azure.source.testdata.AzureOffsetManagerIntegrationTestData;
import io.aiven.kafka.connect.azure.source.config.AzureBlobSourceConfig;
import io.aiven.kafka.connect.azure.source.testutils.AzureBlobAccessor;
import io.aiven.kafka.connect.azure.source.utils.AzureBlobClient;
import io.aiven.kafka.connect.azure.source.utils.AzureBlobOffsetManagerEntry;
import io.aiven.kafka.connect.azure.source.utils.AzureBlobSourceRecordIterator;
import io.aiven.kafka.connect.common.integration.AbstractSourceIteratorIntegrationTest;
import io.aiven.kafka.connect.common.source.OffsetManager;
import io.aiven.kafka.connect.common.source.input.Transformer;
import org.apache.kafka.connect.connector.Connector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

@Testcontainers
public class AzureBlobSourceRecordIteratorIntegrationTest extends AbstractSourceIteratorIntegrationTest<String, AzureBlobOffsetManagerEntry, AzureBlobSourceRecordIterator> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AzureBlobSourceRecordIteratorIntegrationTest.class);

    //    @Container
//    static final LocalStackContainer LOCALSTACK = new LocalStackContainer(DockerImageName.parse("localstack/localstack:2.0.2"))
//            .withServices(LocalStackContainer.Service.S3);
    @Container
    private static final GenericContainer<?> AZURITE_CONTAINER = AzureIntegrationTestData.createContainer();

    AzureIntegrationTestData testData;

    @Override
    protected Logger getLogger() {
        return LOGGER;
    }

    @BeforeEach
    void setupAzure() {
        testData = new AzureIntegrationTestData(AZURITE_CONTAINER);
    }

    @AfterEach
    void tearDownAzure() {
        testData.tearDown();
    }


    /**
     * Creates the native key.
     * @param prefix the prefix for the key.
     * @param topic the topic for the key,
     * @param partition the partition for the key.
     * @return the native Key.
     */
    @Override
    protected String createKey(String prefix, String topic, int partition) {
        return testData.createKey(prefix, topic, partition);
    }

    @Override
    protected List<AzureBlobAccessor.AzureNativeInfo> getNativeStorage() {
        return testData.getNativeStorage();
    }

    @Override
    protected Class<? extends Connector> getConnectorClass() {
        return testData.getConnectorClass();
    }


    @Override
    protected WriteResult<String> writeWithKey(final String nativeKey, final byte[] testDataBytes) {
        return testData.writeWithKey(nativeKey, testDataBytes);
    }

    @Override
    protected Map<String, String> createConnectorConfig(final String localPrefix) {
        return testData.createConnectorConfig(localPrefix);
    }

    @Override
    protected BiFunction<Map<String, Object>, Map<String, Object>, AzureBlobOffsetManagerEntry> offsetManagerEntryFactory() {
        return AzureOffsetManagerIntegrationTestData.offsetManagerEntryFactory();
    }

    @Override
    protected OffsetManager.OffsetManagerKey createOffsetManagerKey(final String nativeKey) {
        return AzureOffsetManagerIntegrationTestData.createOffsetManagerKey(nativeKey);
    }

    @Override
    protected Function<Map<String, Object>, AzureBlobOffsetManagerEntry> getOffsetManagerEntryCreator(OffsetManager.OffsetManagerKey key) {
        return AzureOffsetManagerIntegrationTestData.getOffsetManagerEntryCreator(key);
    }

    @Override
    protected AzureBlobSourceRecordIterator getSourceRecordIterator(Map<String, String> configData, OffsetManager<AzureBlobOffsetManagerEntry> offsetManager, Transformer transformer) {
        AzureBlobSourceConfig sourceConfig = new AzureBlobSourceConfig(configData);
        return new AzureBlobSourceRecordIterator(sourceConfig, offsetManager, transformer, new AzureBlobClient(sourceConfig));
    }
}
