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

package org.apache.datasketches.hive.theta;

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Union;

/**
 * Hive estimate sketch UDF.
 */
@SuppressWarnings("javadoc")
public class SampleSketchUDF extends UDF {
  public static final int DEFAULT_SIZE = 16384;

  /**
   * Main logic called by hive, produces new sketch from original using
   * specified size and sampling probability.
   *
   * @param binarySketch
   *          sketch to be sampled passed in as bytes writable.
   * @param sketchSize
   *          Size to use for the new sketch.
   *          This must be a power of 2 and larger than 16. If zero, DEFAULT is used.
   * @param probability
   *          The sampling probability to use for the new sketch.
   *          Should be greater than zero and less than or equal to 1.0
   * @return The sampled sketch encoded as a BytesWritable
   */
  public BytesWritable evaluate(BytesWritable binarySketch, int sketchSize, float probability) {

    // Null checks
    if (binarySketch == null) {
      return null;
    }

    Memory serializedSketch = BytesWritableHelper.wrapAsMemory(binarySketch);

    if (serializedSketch.getCapacity() <= 8) {
      return null;
    }

    //  The builder will catch errors with improper sketchSize or probability
    Union union = SetOperation.builder().setP(probability).setNominalEntries(sketchSize).buildUnion();

    union.update(serializedSketch); //Union can accept Memory object directly

    Sketch intermediateSketch = union.getResult(false, null); //to CompactSketch(unordered, on-heap)
    byte[] resultSketch = intermediateSketch.toByteArray();

    BytesWritable result = new BytesWritable();
    result.set(resultSketch, 0, resultSketch.length);

    return result;
  }
}
