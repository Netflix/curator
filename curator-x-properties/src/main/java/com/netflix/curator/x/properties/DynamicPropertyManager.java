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

package com.netflix.curator.x.properties;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.utils.EnsurePath;

public class DynamicPropertyManager
{
    private final CuratorFramework client;
    private final String basePath;
    private final EnsurePath ensurePath;

    public DynamicPropertyManager(CuratorFramework client, String basePath)
    {
        this.client = client;
        this.basePath = basePath;
        ensurePath = client.newNamespaceAwareEnsurePath(basePath);
    }
    
    public void         addPropertySource(PropertySource source)
    {
        addPropertySource(source, null);
    }
    
    public void         addPropertySource(PropertySource source, Path forPath)
    {
        // TODO
    }

    public Value<String>    getValue(Property property) throws Exception
    {
        return new ValueImp(this, property, null, null);    // TODO generate source and path
    }

    public Value<Integer>    getIntValue(Property property) throws Exception
    {
        return new ValueWrap<Integer>(getValue(property))
        {
            @Override
            protected Integer convert(String str, Integer defaultValue)
            {
                if ( str != null )
                {
                    try
                    {
                        return Integer.parseInt(str);
                    }
                    catch ( NumberFormatException ignore )
                    {
                        // ignore
                    }
                }
                return defaultValue;
            }
        };
    }

    public Value<Long>    getLongValue(Property property) throws Exception
    {
        return new ValueWrap<Long>(getValue(property))
        {
            @Override
            protected Long convert(String str, Long defaultValue)
            {
                if ( str != null )
                {
                    try
                    {
                        return Long.parseLong(str);
                    }
                    catch ( NumberFormatException ignore )
                    {
                        // ignore
                    }
                }
                return defaultValue;
            }
        };
    }

    public Value<Double>    getDoubleValue(Property property) throws Exception
    {
        return new ValueWrap<Double>(getValue(property))
        {
            @Override
            protected Double convert(String str, Double defaultValue)
            {
                if ( str != null )
                {
                    try
                    {
                        return Double.parseDouble(str);
                    }
                    catch ( NumberFormatException ignore )
                    {
                        // ignore
                    }
                }
                return defaultValue;
            }
        };
    }

    public Value<Boolean>    getBooleanValue(Property property) throws Exception
    {
        return new ValueWrap<Boolean>(getValue(property))
        {
            @Override
            protected Boolean convert(String str, Boolean defaultValue)
            {
                if ( (str != null) && (str.length() > 0) )
                {
                    return str.equalsIgnoreCase("true") || !str.equals("0");
                }
                return defaultValue;
            }
        };
    }

    void        internalSet(Property property, String newValue) throws Exception
    {
        // TODO
    }
}
