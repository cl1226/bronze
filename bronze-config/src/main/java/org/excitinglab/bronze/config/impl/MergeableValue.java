package org.excitinglab.bronze.config.impl;

import org.excitinglab.bronze.config.ConfigMergeable;
import org.excitinglab.bronze.config.ConfigValue;

interface MergeableValue extends ConfigMergeable {
    // converts a Config to its root object and a ConfigValue to itself
    ConfigValue toFallbackValue();
}
