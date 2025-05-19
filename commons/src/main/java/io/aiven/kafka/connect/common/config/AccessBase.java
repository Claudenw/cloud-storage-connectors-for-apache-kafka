package io.aiven.kafka.connect.common.config;

import org.apache.kafka.common.config.AbstractConfig;

public interface AccessBase {
    AbstractConfig getAbstractConfig();

    /**
     * Determines if a key has been set.
     *
     * @param key
     *            The key to check.
     * @return {@code true} if the key was set, {@code false} if the key was not set or does not exist in the config.
     */
    default boolean has(final String key) {
        return getAbstractConfig().values().get(key) != null;
    }
}
