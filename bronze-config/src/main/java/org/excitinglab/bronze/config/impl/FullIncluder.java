/**
 *   Copyright (C) 2011-2012 Typesafe Inc. <http://typesafe.com>
 */
package org.excitinglab.bronze.config.impl;

import org.excitinglab.bronze.config.ConfigIncluder;
import org.excitinglab.bronze.config.ConfigIncluderClasspath;
import org.excitinglab.bronze.config.ConfigIncluderFile;
import org.excitinglab.bronze.config.ConfigIncluderURL;

interface FullIncluder extends ConfigIncluder, ConfigIncluderFile, ConfigIncluderURL,
            ConfigIncluderClasspath {

}
