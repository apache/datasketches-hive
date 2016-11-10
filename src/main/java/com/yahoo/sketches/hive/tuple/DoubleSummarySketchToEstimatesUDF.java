/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.tuple;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.tuple.DoubleSummary;
import com.yahoo.sketches.tuple.Sketch;
import com.yahoo.sketches.tuple.SketchIterator;
import com.yahoo.sketches.tuple.Sketches;

@Description(
    name = "DoubleSummarySketchToEstimates",
    value = "_FUNC_(sketch)",
    extended = "Returns a list of estimates from a given Sketch<DoubleSummary>."
    + " The result will be two double values."
    + " The first estimate is the estimate of the number of unique keys in the"
    + " original population. Next there is an estimate of the sum of the parameter in the"
    + " original population (sum of the values in the sketch scaled to the original population."
    + " This estimate assumes that the DoubleSummary was used in the Sum mode.)")
public class DoubleSummarySketchToEstimatesUDF extends UDF {

  /**
   * Get estimates from a given Sketch&lt;DoubleSummary&gt;
   * @param serializedSketch DoubleSummarySketch in as serialized binary
   * @return list of estimates
   */
  public List<Double> evaluate(final BytesWritable serializedSketch) {
    if (serializedSketch == null) return null;
    final Sketch<DoubleSummary> sketch = Sketches.heapifySketch(new NativeMemory(serializedSketch.getBytes()));
    double sum = 0;
    final SketchIterator<DoubleSummary> it = sketch.iterator();
    while (it.next()) {
      sum += it.getSummary().getValue();
    }
    return Arrays.asList(
      sketch.getEstimate(),
      sum / sketch.getTheta()
    );
  }

}
