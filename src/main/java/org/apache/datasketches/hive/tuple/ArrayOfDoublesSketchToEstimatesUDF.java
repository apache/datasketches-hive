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

import java.util.ArrayList;
import java.util.List;

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.tuple.ArrayOfDoublesSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesSketchIterator;
import org.apache.datasketches.tuple.ArrayOfDoublesSketches;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

@Description(
    name = "ArrayOfDoublesSketchToEstimates",
    value = "_FUNC_(sketch)",
    extended = "Returns a list of estimates from a given ArrayOfDoublesSketch."
    + " The result will be N+1 double values, where N is the number of double values kept in the"
    + " sketch per key. The first estimate is the estimate of the number of unique keys in the"
    + " original population. Next there are N estimates of the sums of the parameters in the"
    + " original population (sums of the values in the sketch scaled to the original population)")
@SuppressWarnings("javadoc")
public class ArrayOfDoublesSketchToEstimatesUDF extends UDF {

  /**
   * Get estimates from a given ArrayOfDoublesSketch
   * @param serializedSketch ArrayOfDoublesSketch in as serialized binary
   * @return list of estimates
   */
  public List<Double> evaluate(final BytesWritable serializedSketch) {
    if (serializedSketch == null) { return null; }
    final ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.wrapSketch(
        BytesWritableHelper.wrapAsMemory(serializedSketch));
    final double[] sums = new double[sketch.getNumValues()];
    final ArrayOfDoublesSketchIterator it = sketch.iterator();
    while (it.next()) {
      final double[] values = it.getValues();
      for (int i = 0; i < sketch.getNumValues(); i++) {
         sums[i] += values[i];
      }
    }
    final List<Double> estimates = new ArrayList<>(sketch.getNumValues() + 1);
    estimates.add(sketch.getEstimate());
    for (int i = 0; i < sums.length; i++) {
      estimates.add(sums[i] / sketch.getTheta());
    }
    return estimates;
  }

}
