/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.curator.x.discovery.details;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.base.Preconditions;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.ServiceInstanceBuilder;
import com.netflix.curator.x.discovery.ServiceType;
import com.netflix.curator.x.discovery.UriSpec;
import com.netflix.curator.x.discovery.details.InstanceSerializer;
import com.netflix.curator.x.discovery.details.JsonInstanceSerializer;

/**
 * A serializer that uses Jackson to serialize/deserialize as JSON. IMPORTANT: The instance
 * payload must support Jackson
 */
public class JsonInstanceSerializer<T> implements InstanceSerializer<T>
{

    private final ObjectMapper mMapper;
    private final Class<T> mPayloadClass;

    /**
     * @param payloadClass
     *            used to validate payloads when deserializing
     */
    public JsonInstanceSerializer(final Class<T> payloadClass) {
        this(payloadClass, new ObjectMapper());
    }

    public JsonInstanceSerializer(final Class<T> pPayloadClass, final ObjectMapper pMapper) {
        mPayloadClass = pPayloadClass;
        mMapper = pMapper;
        mMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
	public byte[] serialize(final ServiceInstance<T> pInstance) throws Exception {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        mMapper.writeValue(out, pInstance);
        return out.toByteArray();

    }

    private String getTextField(final JsonNode pNode, final String pFieldName) {
        Preconditions.checkNotNull(pNode);
        Preconditions.checkNotNull(pFieldName);
        return pNode.get(pFieldName) != null ? pNode.get(pFieldName).getTextValue() : null;
    }

    private Integer getIntegerField(final JsonNode pNode, final String pFieldName) {
        Preconditions.checkNotNull(pNode);
        Preconditions.checkNotNull(pFieldName);
        return (pNode.get(pFieldName) != null && pNode.get(pFieldName).isNumber()) ? pNode.get(pFieldName)
	    .getIntValue() : null;
    }

    private Long getLongField(final JsonNode pNode, final String pFieldName) {
        Preconditions.checkNotNull(pNode);
        Preconditions.checkNotNull(pFieldName);
        return (pNode.get(pFieldName) != null && pNode.get(pFieldName).isLong()) ? pNode.get(pFieldName).getLongValue()
	    : null;
    }

    private <O> O getObject(final JsonNode pNode, final String pFieldName, final Class<O> pObjectClass)
	throws JsonParseException, JsonMappingException, IOException {
        Preconditions.checkNotNull(pNode);
        Preconditions.checkNotNull(pFieldName);
        Preconditions.checkNotNull(pObjectClass);
        if (pNode.get(pFieldName) != null && pNode.get(pFieldName).isObject()) {
            return mMapper.readValue(pNode.get(pFieldName), pObjectClass);
        } else {
            return null;
        }
    }

    @Override
	public ServiceInstance<T> deserialize(final byte[] pBytes) throws Exception {
        final ByteArrayInputStream bais = new ByteArrayInputStream(pBytes);
        final JsonNode rootNode = mMapper.readTree(bais);
        final ServiceInstanceBuilder<T> builder = ServiceInstance.builder();
        {
            final String address = getTextField(rootNode, "address");
            if (address != null) {
                builder.address(address);
            }
        }
        {
            final String id = getTextField(rootNode, "id");
            if (id != null) {
                builder.id(id);
            }
        }
        {
            final String name = getTextField(rootNode, "name");
            if (name != null) {
                builder.name(name);
            }
        }
        {
            final Integer port = getIntegerField(rootNode, "port");
            if (port != null) {
                builder.port(port);
            }
        }
        {
            final Integer sslPort = getIntegerField(rootNode, "sslPort");
            if (sslPort != null) {
                builder.sslPort(sslPort);
            }
        }
        {
            final Long registrationTimeUTC = getLongField(rootNode, "registrationTimeUTC");
            if (registrationTimeUTC != null) {
                builder.registrationTimeUTC(registrationTimeUTC);
            }
        }
        {
            final T payload = getObject(rootNode, "payload", mPayloadClass);
            if (payload != null) {
                builder.payload(payload);
            }
        }
        {
            final ServiceType serviceType = getObject(rootNode, "serviceType", ServiceType.class);
            if (serviceType != null) {
                builder.serviceType(serviceType);
            }
        }
        {
            final UriSpec uriSpec = getObject(rootNode, "uriSpec", UriSpec.class);
            if (uriSpec != null) {
                builder.uriSpec(uriSpec);
            }
        }
        return builder.build();
    }

}
