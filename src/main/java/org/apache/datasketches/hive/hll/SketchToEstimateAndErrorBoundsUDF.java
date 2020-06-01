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

package org.apache.datasketches.hive.hll;

import java.util.Arrays;
import java.util.List;

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.hll.HllSketch;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

@Description(
    name = "SketchToEstimateAndErrorBounds",
    value = "_FUNC_(sketch, kappa)",
    extended = "Returns an estimate of distinct count and error bounds from a given HllSketch."
    + " Optional kappa is a number of standard deviations from the mean: 1, 2 or 3 (default 2)."
    + " The result is three double values: estimate, lower bound and upper bound.")
@SuppressWarnings("javadoc")
public class SketchToEstimateAndErrorBoundsUDF extends UDF {

  private static final int DEFAULT_KAPPA = 2;

  /**
   * Get an estimate and error bounds from a given HllSketch with default kappa
   * @param serializedSketch HllSketch in a serialized binary form
   * @return estimate and bounds
   */
  public List<Double> evaluate(final BytesWritable serializedSketch) {
    return evaluate(serializedSketch, DEFAULT_KAPPA);
  }

  /**
   * Get an estimate and error bounds from a given HllSketch with explicit kappa
   * @param serializedSketch HllSketch in a serialized binary form
   * @param kappa given number of standard deviations from the mean: 1, 2 or 3
   * @return estimate and bounds
   */
  public List<Double> evaluate(final BytesWritable serializedSketch, final int kappa) {
    if (serializedSketch == null) { return null; }
    final HllSketch sketch = HllSketch.wrap(BytesWritableHelper.wrapAsMemory(serializedSketch));
    return Arrays.asList(sketch.getEstimate(), sketch.getLowerBound(kappa), sketch.getUpperBound(kappa));
  }

}
