/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.realitygames.processors.nifi;

import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;
import org.apache.nifi.processors.standard.util.TransformFactory;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;
import org.apache.commons.lang.StringUtils;

import com.bazaarvoice.jolt.Transform;
import com.bazaarvoice.jolt.JsonUtils;
import com.bazaarvoice.jolt.Chainr;
import com.jayway.jsonpath.JsonPath;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"json", "array", "join"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttribute(attribute = "mime.type",description = "Always set to application/json")
@CapabilityDescription("Joins a JSON array to an attribute by the given JSON path")
public class JSONArrayJoin extends AbstractProcessor {

    public static final PropertyDescriptor JSON_PATH = new PropertyDescriptor.Builder()
            .name("json-path")
            .displayName("JSON Path")
            .description("JSON path which contains the desired array to join")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();
    public static final PropertyDescriptor TARGET_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("target-attribute")
            .displayName("Target attribute")
            .description("Attribute to join the given JSON array to")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The FlowFile with transformed content will be routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile fails processing for any reason (for example, the FlowFile is not valid JSON), it will be routed to this relationship")
            .build();

    private final static List<PropertyDescriptor> properties;
    private final static Set<Relationship> relationships;
    private final static String DEFAULT_CHARSET = "UTF-8";

    private volatile String jsonPath;
    private volatile String targetAttribute;

    static{

        final List<PropertyDescriptor> _properties = new ArrayList<>();
        _properties.add(JSON_PATH);
        _properties.add(TARGET_ATTRIBUTE);
        properties = Collections.unmodifiableList(_properties);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);

    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, ProcessSession session) throws ProcessException {

        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final StopWatch stopWatch = new StopWatch(true);

        final byte[] originalContent = new byte[(int) original.getSize()];
        session.read(original, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, originalContent, true);
            }
        });

        final List<String> result;

        try {
            final ByteArrayInputStream bais = new ByteArrayInputStream(originalContent);
            result = JsonPath.read(bais, jsonPath);
        } catch (RuntimeException re) {
            logger.error("Unable to transform {} due to {}", new Object[]{original, re});
            session.transfer(original, REL_FAILURE);
            return;
        } catch (IOException ie) {
            logger.error("Unable to transform {} due to {}", new Object[]{original, ie});
            session.transfer(original, REL_FAILURE);
            return;
        }finally {
        }

        FlowFile transformed;

        transformed = session.putAttribute(original, CoreAttributes.MIME_TYPE.key(),"application/json");
        transformed = session.putAttribute(transformed, targetAttribute, String.join(".", result));
        session.transfer(transformed, REL_SUCCESS);
        session.getProvenanceReporter().modifyContent(transformed,"Modified With JSONArrayJoin", stopWatch.getElapsed(TimeUnit.MILLISECONDS));
        logger.info("Transformed {}", new Object[]{original});

    }

    @OnScheduled
    public void setup(final ProcessContext context) {

        try{
            jsonPath = context.getProperty(JSON_PATH).getValue();
            targetAttribute = context.getProperty(TARGET_ATTRIBUTE).getValue();

        } catch (Exception ex){
            getLogger().error("Unable to setup processor",ex);
        }

    }

    protected FilenameFilter getJarFilenameFilter(){
        return (dir, name) -> (name != null && name.endsWith(".jar"));
    }

}