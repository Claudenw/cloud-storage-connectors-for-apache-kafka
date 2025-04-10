package io.aiven.kafka.connect.azure.source.testdata;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import io.aiven.kafka.connect.azure.source.AzureBlobSourceConnector;
import io.aiven.kafka.connect.azure.source.config.AzureBlobConfigFragment;
import io.aiven.kafka.connect.azure.source.utils.AzureBlobOffsetManagerEntry;
import io.aiven.kafka.connect.common.config.SourceConfigFragment;
import io.aiven.kafka.connect.common.integration.AbstractIntegrationTest;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.connector.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.azure.AzuriteContainer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Manages test data
 */
public final class AzureIntegrationTestData {
    private static final Logger LOGGER = LoggerFactory.getLogger(AzureIntegrationTestData.class);
    static final String DEFAULT_CONTAINER = "test-container";
    private static final String ACCOUNT_NAME = "devstoreaccount1";
    private static final String ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    private final AzuriteContainer container;
    private final BlobServiceClient azureServiceClient;
    private final ContainerAccessor containerAccessor;

    /**
     * Constructor.
     * @param container the container to Azure read/wrtie to.
     */
    public AzureIntegrationTestData(final AzuriteContainer container) {
        this.container = container;
        azureServiceClient = new BlobServiceClientBuilder().connectionString(container.getConnectionString()).buildClient();
        LOGGER.info("Azure blob service {} client created", azureServiceClient.getServiceVersion());
        containerAccessor = getContainerAccessor(DEFAULT_CONTAINER);
        containerAccessor.createContainer();
    }

    /**
     * Get a container accessor.
     * @param name the name of the container.
     * @return a Container accessor.
     */
    public ContainerAccessor getContainerAccessor(final String name) {
        return new ContainerAccessor(azureServiceClient, name);
    }

    /**
     * Tear down this accessor.
     */
    public void tearDown() {
        containerAccessor.removeContainer();
    }

    /**
     * Creates the Azurite container for testing.
     * @return a newly constructed Azurite container.
     */
    public static AzuriteContainer createContainer() {
        return new AzuriteContainer("mcr.microsoft.com/azure-storage/azurite:3.33.0");
    }

    /**
     * Creates a native key.
     * @param prefix the prefix for the key.
     * @param topic the  topic for the key.
     * @param partition the partition for the key.
     * @return the native key.
     */
    public String createKey(final String prefix, final String topic, final int partition) {
        return format("%s%s-%05d-%d.txt", StringUtils.defaultIfBlank(prefix, ""), topic, partition, System.currentTimeMillis());
    }

    /**
     * Writes data to the default container.
     * @param nativeKey the native key to write
     * @param testDataBytes the data to write.
     * @return the WriteResults.
     */
    public AbstractIntegrationTest.WriteResult<String> writeWithKey(final String nativeKey, final byte[] testDataBytes) {
        containerAccessor.getBlobClient(nativeKey).upload(new ByteArrayInputStream(testDataBytes));
        return new AbstractIntegrationTest.WriteResult<>(new AzureBlobOffsetManagerEntry(containerAccessor.getContainerName(), nativeKey).getManagerKey(), nativeKey);
    }

    /**
     * Gets the native storage information.
     * @return the native storage information.
     */
    public List<ContainerAccessor.AzureNativeInfo> getNativeStorage() {
        return containerAccessor.getNativeStorage();
    }

    /**
     * Gets the connector class.
     * @return the connector class.
     */
    public Class<? extends Connector> getConnectorClass() {
        return AzureBlobSourceConnector.class;
    }

    /**
     * Creates the connector config with the specified local prefix.
     * @param localPrefix the prefix to prepend to all keys.  May be {@code nul}.
     * @return the map of data options.
     */
    public Map<String, String> createConnectorConfig(final String localPrefix) {
        return createConnectorConfig(localPrefix, DEFAULT_CONTAINER);
    }

    /**
     * Creates the connector config with the specified local prefix and container.
     * @param localPrefix the prefix to prepend to all keys.  May be {@code nul}.
     * @param containerName the container name to use.
     * @return the map of data options.
     */
    public Map<String, String> createConnectorConfig(String localPrefix, String containerName) {
        Map<String, String> data = new HashMap<>();

        SourceConfigFragment.setter(data)
                .ringBufferSize(10);

        AzureBlobConfigFragment.Setter setter =  AzureBlobConfigFragment.setter(data)
                .containerName(containerName)
                .accountName(ACCOUNT_NAME)
                .accountKey(ACCOUNT_KEY)
                .endpointProtocol(AzureBlobConfigFragment.Protocol.HTTP)
                .connectionString(container.getConnectionString())
                .containerName(containerName);
        if (localPrefix != null) {
            setter.prefix(localPrefix);
        }
        return data;
    }
}
