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

import com.google.common.collect.Ordering;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;

public class Property implements Iterable<PropertyAttribute>
{
    private final String                         name;
    private final Scope                          scope;
    private final Collection<PropertyAttribute>  attributes;

    private static final Comparator<PropertyAttribute> COMPARATOR = new Comparator<PropertyAttribute>()
    {
        @Override
        public int compare(PropertyAttribute left, PropertyAttribute right)
        {
            int diff = left.getPriority() - right.getPriority();
            return (diff < 0) ? -1 : ((diff > 0) ? 1 : 0);
        }
    };

    public Property(String name, Scope scope, Collection<PropertyAttribute> attributes)
    {
        this.name = name;
        this.scope = scope;
        this.attributes = Ordering.from(COMPARATOR).immutableSortedCopy(attributes);
    }

    public String getName()
    {
        return name;
    }

    public Scope getScope()
    {
        return scope;
    }

    @Override
    public Iterator<PropertyAttribute> iterator()
    {
        return attributes.iterator();
    }

    public int      attributeQty()
    {
        return attributes.size();
    }
}
