/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.tuple;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketchIterator;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketches;

@Description(
    name = "ArrayOfDoublesSketchToEstimates",
    value = "_FUNC_(sketch)",
    extended = "Returns a list of estimates from a given ArrayOfDoublesSketch."
    + " The result will be N+1 double values, where N is the number of double values kept in the"
    + " sketch per key. The first estimate is the estimate of the number of unique keys in the"
    + " original population. Next there are N estimates of the sums of the parameters in the"
    + " original population (sums of the values in the sketch scaled to the original population)")
public class ArrayOfDoublesSketchToEstimatesUDF extends UDF {

  /**
   * Get estimates from a given ArrayOfDoublesSketch
   * @param serializedSketch ArrayOfDoublesSketch in as serialized binary
   * @return list of estimates
   */
  public List<Double> evaluate(final BytesWritable serializedSketch) {
    if (serializedSketch == null) return null;
    final ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.wrapSketch(
        new NativeMemory(serializedSketch.getBytes()));
    final double[] sums = new double[sketch.getNumValues()];
    final ArrayOfDoublesSketchIterator it = sketch.iterator();
    while (it.next()) {
      final double[] values = it.getValues();
      for (int i = 0; i < sketch.getNumValues(); i++) {
         sums[i] += values[i];
      }
    }
    final List<Double> estimates = new ArrayList<Double>(sketch.getNumValues() + 1);
    estimates.add(sketch.getEstimate());
    for (int i = 0; i < sums.length; i++) {
      estimates.add(sums[i] / sketch.getTheta());
    }
    return estimates;
  }

}
