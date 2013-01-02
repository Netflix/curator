/*
 * Copyright 2013 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.curator.framework.imps;

import com.netflix.curator.framework.api.CompressionProvider;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GzipCompressionProvider implements CompressionProvider
{
    @Override
    public byte[] compress(String path, byte[] data) throws Exception
    {
        ByteArrayOutputStream       bytes = new ByteArrayOutputStream();
        GZIPOutputStream            out = new GZIPOutputStream(bytes);
        out.write(data);
        out.finish();
        return bytes.toByteArray();
    }

    @Override
    public byte[] decompress(String path, byte[] compressedData) throws Exception
    {
        ByteArrayOutputStream       bytes = new ByteArrayOutputStream(compressedData.length);
        GZIPInputStream             in = new GZIPInputStream(new ByteArrayInputStream(compressedData));
        byte[]                      buffer = new byte[compressedData.length];
        for(;;)
        {
            int     bytesRead = in.read(buffer, 0, buffer.length);
            if ( bytesRead < 0 )
            {
                break;
            }
            bytes.write(buffer, 0, bytesRead);
        }
        return bytes.toByteArray();
    }
}
