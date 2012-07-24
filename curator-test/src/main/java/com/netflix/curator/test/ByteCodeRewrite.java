/*
 * Copyright 2012 Netflix, Inc.
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

package com.netflix.curator.test;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.NotFoundException;

public class ByteCodeRewrite
{
    public static void      apply()
    {
        // NOP - only needed so that static initializer is run
    }

    static
    {
        /*
            This ugliness is necessary. There is no way to tell ZK to not register JMX beans. Something
            in the shutdown of a QuorumPeer causes the state of the MBeanRegistry to get confused and
            generates an assert Exception.
         */
        ClassPool pool = ClassPool.getDefault();
        try
        {
            pool.appendClassPath(new javassist.LoaderClassPath(TestingCluster.class.getClassLoader()));     // re: https://github.com/Netflix/curator/issues/11

            try
            {
                CtClass cc = pool.get("org.apache.zookeeper.server.ZooKeeperServer");
                fixMethods(cc);
            }
            catch ( NotFoundException ignore )
            {
                // ignore
            }

            try
            {
                CtClass cc = pool.get("org.apache.zookeeper.server.quorum.LearnerZooKeeperServer");
                fixMethods(cc);
            }
            catch ( NotFoundException ignore )
            {
                // ignore
            }
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
    }

    private static void fixMethods(CtClass cc) throws CannotCompileException
    {
        for ( CtMethod method : cc.getDeclaredMethods() )
        {
            if ( method.getName().equals("registerJMX") || method.getName().equals("unregisterJMX") )
            {
                method.setBody(null);
            }
        }
        cc.toClass();
    }
}
