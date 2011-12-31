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

public class PropertyAttribute
{
    private final String        name;
    private final String        value;
    private final int           priority;
    private final boolean       optional;

    public PropertyAttribute(String name, String value, int priority, boolean optional)
    {
        this.name = name;
        this.value = value;
        this.priority = priority;
        this.optional = optional;
    }

    public String getName()
    {
        return name;
    }

    public String getValue()
    {
        return value;
    }

    public int getPriority()
    {
        return priority;
    }

    public boolean isOptional()
    {
        return optional;
    }
}
