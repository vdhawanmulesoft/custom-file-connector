package org.mule.extension.internal;

import org.mule.extension.internal.source.CustomFileSource;
import org.mule.runtime.extension.api.annotation.Extension;
import org.mule.runtime.extension.api.annotation.Sources;
import org.mule.runtime.extension.api.annotation.dsl.xml.Xml;


/**
 * This is the main class of an extension, is the entry point from which configurations, connection providers, operations
 * and sources are going to be declared.
 */
@Xml(prefix = "basic")
@Extension(name = "Basic")
@Sources(CustomFileSource.class)
public class BasicExtension {

}
