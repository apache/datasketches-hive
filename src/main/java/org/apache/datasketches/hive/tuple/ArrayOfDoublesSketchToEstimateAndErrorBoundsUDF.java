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

package org.apache.datasketches.hive.tuple;

import java.util.Arrays;
import java.util.List;

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.tuple.ArrayOfDoublesSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesSketches;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

@Description(
    name = "ArrayOfDoublesSketchToEstimateAndErrorBounds",
    value = "_FUNC_(sketch)",
    extended = "Returns a unique count estimate and error bounds from a given ArrayOfDoublesSketch."
    + " The result will be three double values:"
    + " estimate of the number of unique keys, lower bound and upper bound. The bounds are given"
    + " at 95.5% confidence.")
@SuppressWarnings("javadoc")
public class ArrayOfDoublesSketchToEstimateAndErrorBoundsUDF extends UDF {

  /**
   * Get estimate, lower and upper bounds from a given ArrayOfDoublesSketch
   * @param serializedSketch ArrayOfDoublesSketch in as serialized binary
   * @return estimate, lower bound and upper bound
   */
  public List<Double> evaluate(final BytesWritable serializedSketch) {
    if (serializedSketch == null) { return null; }
    final ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.wrapSketch(
        BytesWritableHelper.wrapAsMemory(serializedSketch));
    return Arrays.asList(sketch.getEstimate(), sketch.getLowerBound(2), sketch.getUpperBound(2));
  }

}
