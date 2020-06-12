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

import java.util.Arrays;
import java.util.List;

import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

@Description(
    name = "GetEstimateAndBounds",
    value = "_FUNC_(sketch, kappa, seed)",
    extended = "Returns an estimate and bounds of unique count from a given CpcSketch."
    + " The result is three double values: estimate, lower bound and upper bound."
    + " Optional kappa is a number of standard deviations from the mean: 1, 2 or 3 (default 2)."
    + " The seed is optional. It is needed if the sketch was created with a custom seed.")
@SuppressWarnings("javadoc")
public class GetEstimateAndErrorBoundsUDF extends UDF {

  private static final int DEFAULT_KAPPA = 2;

  /**
   * Get an estimate from a given CpcSketch using the default seed and kappa
   * @param serializedSketch CpcSketch in a serialized binary form
   * @return estimate of unique count
   */
  public List<Double> evaluate(final BytesWritable serializedSketch) {
    return evaluate(serializedSketch, DEFAULT_KAPPA, DEFAULT_UPDATE_SEED);
  }

  /**
   * Get an estimate and bounds from a given CpcSketch with default seed
   * @param serializedSketch CpcSketch in a serialized binary form
   * @param kappa given number of standard deviations from the mean: 1, 2 or 3
   * @return estimate, lower and upper bound
   */
  public List<Double> evaluate(final BytesWritable serializedSketch, final int kappa) {
    return evaluate(serializedSketch, kappa, DEFAULT_UPDATE_SEED);
  }

  /**
   * Get an estimate and bounds from a given CpcSketch with explicit seed
   * @param serializedSketch CpcSketch in a serialized binary form
   * @param kappa given number of standard deviations from the mean: 1, 2 or 3
   * @param seed update seed
   * @return estimate, lower and upper bound
   */
  public List<Double> evaluate(final BytesWritable serializedSketch, final int kappa, final long seed) {
    if (serializedSketch == null) { return null; }
    final CpcSketch sketch = CpcSketch.heapify(BytesWritableHelper.wrapAsMemory(serializedSketch), seed);
    return Arrays.asList(sketch.getEstimate(), sketch.getLowerBound(kappa), sketch.getUpperBound(kappa));
  }

}
