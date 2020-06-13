/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.hive.common;

import java.nio.ByteOrder;

import org.apache.datasketches.memory.Memory;
import org.apache.hadoop.io.BytesWritable;

/**
 * Provides a helper class to simplify frequent operations on BytesWritable.
 */
public class BytesWritableHelper {
    /**
     * Wraps BytesWritable with a read-only Memory interface, without copying the underlying data.
     * @param bw Input BytesWritable object
     * @return Read-only Memory wrapping the input BytesWritable
     */
    public static Memory wrapAsMemory(final BytesWritable bw) {
        return Memory.wrap(bw.getBytes(), 0, bw.getLength(), ByteOrder.nativeOrder());
    }
}
