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

import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.Sketch;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

/**
 * Hive estimate sketch udf. V4
 *
 */
public class EstimateSketchUDF extends UDF {

  private static final int EMPTY_SKETCH_SIZE_BYTES = 8;

  /**
   * Returns the estimate unique count of sketch.
   *
   * @param binarySketch sketch to be estimated passed in as bytes writable.
   * @return the estimate of unique count from given sketch.
   */
  public Double evaluate(final BytesWritable binarySketch) {
    return evaluate(binarySketch, DEFAULT_UPDATE_SEED);
  }

  /**
   * Returns the estimate unique count of sketch.
   *
   * @param binarySketch sketch to be estimated passed in as bytes writable.
   * @param seed value used to build the sketch if different from the default
   * @return the estimate of unique count from given sketch.
   */
  public Double evaluate(final BytesWritable binarySketch, final long seed) {
    if (binarySketch == null) {
      return 0.0;
    }

    final Memory serializedSketch = BytesWritableHelper.wrapAsMemory(binarySketch);

    if (serializedSketch.getCapacity() <= EMPTY_SKETCH_SIZE_BYTES) {
      return 0.0;
    }

    return Sketch.wrap(serializedSketch, seed).getEstimate();
  }

}
