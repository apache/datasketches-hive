/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.tuple;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketches;

@Description(
    name = "ArrayOfDoublesSketchToVariances",
    value = "_FUNC_(sketch)",
    extended = "Returns a list of variance values from a given ArrayOfDoublesSketch."
    + " The result will be N double values, where N is the number of double values kept in the"
    + " sketch per key.")
@SuppressWarnings("javadoc")
public class ArrayOfDoublesSketchToVariancesUDF extends UDF {

  /**
   * Get variances from a given ArrayOfDoublesSketch
   * @param serializedSketch ArrayOfDoublesSketch in as serialized binary
   * @return list of variances
   */
  public List<Double> evaluate(final BytesWritable serializedSketch) {
    if (serializedSketch == null) { return null; }
    final ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.wrapSketch(
        Memory.wrap(serializedSketch.getBytes()));

    if (sketch.getRetainedEntries() < 1) {
      return null;
    }

    final SummaryStatistics[] summaries = ArrayOfDoublesSketchStats.sketchToSummaryStatistics(sketch);
    final List<Double> variances = new ArrayList<>(sketch.getNumValues());
    for (int i = 0; i < sketch.getNumValues(); i++) {
      variances.add(summaries[i].getVariance());
    }
    return variances;
  }

}
