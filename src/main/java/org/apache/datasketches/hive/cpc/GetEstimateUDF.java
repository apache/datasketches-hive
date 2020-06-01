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

package org.apache.datasketches.hive.cpc;

import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;

import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

@Description(
    name = "GetEstimate",
    value = "_FUNC_(sketch)",
    extended = "Returns an estimate of unique count from a given CpcSketch."
    + " The result is a double value.")
@SuppressWarnings("javadoc")
public class GetEstimateUDF extends UDF {

  /**
   * Get an estimate from a given CpcSketch with explicit seed
   * @param serializedSketch CpcSketch in a serialized binary form
   * @param seed update seed
   * @return estimate of unique count
   */
  public Double evaluate(final BytesWritable serializedSketch, final long seed) {
    if (serializedSketch == null) { return null; }
    final CpcSketch sketch = CpcSketch.heapify(BytesWritableHelper.wrapAsMemory(serializedSketch), seed);
    return sketch.getEstimate();
  }

  /**
   * Get an estimate from a given CpcSketch using the default seed
   * @param serializedSketch CpcSketch in a serialized binary form
   * @return estimate of unique count
   */
  public Double evaluate(final BytesWritable serializedSketch) {
    return evaluate(serializedSketch, DEFAULT_UPDATE_SEED);
  }

}
