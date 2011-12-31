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

import com.google.common.collect.Lists;
import java.util.List;

public class PathBuilder
{
    private final List<String> path = Lists.newArrayList();
    
    PathBuilder()
    {
    }
    
    public PathBuilder  add(String name)
    {
        path.add(name);
        return this;
    }
    
    public PathBuilder  add(Path addPath)
    {
        for ( String name : addPath )
        {
            path.add(name);
        }
        return this;
    }

    public PathBuilder  replace(String name)
    {
        path.set(path.size() - 1, name);
        return this;
    }

    public PathBuilder  remove()
    {
        path.remove(path.size() - 1);
        return this;
    }

    public String       get()
    {
        return path.get(path.size() - 1);
    }

    public Path         build()
    {
        return new Path(path);
    }
}
