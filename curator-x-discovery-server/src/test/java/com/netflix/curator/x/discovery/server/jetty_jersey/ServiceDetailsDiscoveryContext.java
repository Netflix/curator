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

package com.netflix.curator.x.discovery.server.jetty_jersey;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

import com.netflix.curator.x.discovery.ProviderStrategy;
import com.netflix.curator.x.discovery.ServiceDiscovery;
import com.netflix.curator.x.discovery.server.contexts.GenericDiscoveryContext;
import com.netflix.curator.x.discovery.server.rest.DiscoveryContext;

/**
 * A DiscoveryContext that maps a concrete payload object of ServiceDetails
 */
@Provider
public class ServiceDetailsDiscoveryContext extends GenericDiscoveryContext<ServiceDetails> implements DiscoveryContext<ServiceDetails>, ContextResolver<DiscoveryContext<ServiceDetails>>
{
    public ServiceDetailsDiscoveryContext(ServiceDiscovery<ServiceDetails> serviceDiscovery, ProviderStrategy<ServiceDetails> providerStrategy, int instanceRefreshMs)
    {
    	super(serviceDiscovery, providerStrategy, instanceRefreshMs, ServiceDetails.class);
    	
    }
}
