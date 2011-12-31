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

import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class Path implements Iterable<String>, Comparable<Path>
{
    private final List<String>      path;

    private static final String       DEFAULT_SEPARATOR = ".";

    public static PathBuilder       builder()
    {
        return new PathBuilder();
    }

    public static Path of(String node)
    {
        return new Path(node);
    }

    public static Path of(String parent, String child)
    {
        return new Path(parent, child);
    }

    public static Path of(String... nodes)
    {
        return new Path(nodes);
    }

    public static Path of(Collection<String> path)
    {
        return new Path(path);
    }

    public Path(String node)
    {
        this.path = ImmutableList.of(node);
    }

    public Path(String parent, String child)
    {
        this.path = ImmutableList.of(parent, child);
    }

    public Path(String... nodes)
    {
        this.path = ImmutableList.copyOf(nodes);
    }

    public Path(Collection<String> path)
    {
        this.path = ImmutableList.copyOf(path);
    }

    @Override
    public Iterator<String> iterator()
    {
        return path.iterator();
    }

    public int  size()
    {
        return path.size();
    }

    public String   get(int n)
    {
        return path.get(n);
    }

    public Path     subPath(int fromIndex)
    {
        return subPath(fromIndex, size());
    }

    public Path     subPath(int fromIndex, int toIndex)
    {
        return new Path(path.subList(fromIndex, toIndex));
    }

    @Override
    public int compareTo(Path rhs)
    {
        return toString().compareTo(rhs.toString());
    }

    public boolean  startsWith(Path rhs)
    {
        //noinspection SimplifiableIfStatement
        if ( rhs.size() <= size() )
        {
            return subPath(0, rhs.size()).equals(rhs);
        }
        return false;
    }
    
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object obj)
    {
        //noinspection SimplifiableIfStatement
        if ( this == obj )
        {
            return true;
        }
        return toString().equals(String.valueOf(obj));
    }

    @Override
    public int hashCode()
    {
        return toString().hashCode();
    }

    @Override
    public String toString()
    {
        return toString(DEFAULT_SEPARATOR);
    }

    public String toString(String separator)
    {
        String              localSeparator = "";
        StringBuilder       str = new StringBuilder();
        for ( String node : path )
        {
            str.append(localSeparator).append(node);
            localSeparator = separator;
        }
        return str.toString();
    }
}
