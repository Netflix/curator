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

abstract class ValueWrap<T> implements Value<T>
{
    private final Value<String> stringValue;

    protected ValueWrap(Value<String> stringValue)
    {
        this.stringValue = stringValue;
    }

    @Override
    public T get() throws Exception
    {
        return get(null);
    }

    @Override
    public T get(T defaultValue) throws Exception
    {
        return convert(stringValue.get(), defaultValue);
    }

    @Override
    public void set(T newValue) throws Exception
    {
        stringValue.set((newValue != null) ? String.valueOf(newValue) : null);
    }

    @Override
    public boolean exists() throws Exception
    {
        return stringValue.exists();
    }
    
    protected abstract T  convert(String str, T defaultValue);
}
