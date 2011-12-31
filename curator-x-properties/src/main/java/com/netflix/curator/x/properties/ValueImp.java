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

class ValueImp implements Value<String>
{
    private final DynamicPropertyManager manager;
    private final Property property;
    private final PropertySource source;
    private final Path path;

    ValueImp(DynamicPropertyManager manager, Property property, PropertySource source, Path path)
    {
        this.manager = manager;
        this.property = property;
        this.source = source;
        this.path = path;
    }

    @Override
    public String get() throws Exception
    {
        return get(null);
    }

    @Override
    public String get(String defaultValue) throws Exception
    {
        return exists() ? source.getProperty(path) : defaultValue;
    }

    @Override
    public void set(String newValue) throws Exception
    {
        manager.internalSet(property, newValue);
    }

    @Override
    public boolean exists() throws Exception
    {
        return source.exists(path);
    }
}
