/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.tuple;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.UpdateDoublesSketch;
import com.yahoo.sketches.tuple.DoubleSummary;
import com.yahoo.sketches.tuple.Sketch;
import com.yahoo.sketches.tuple.SketchIterator;
import com.yahoo.sketches.tuple.Sketches;

@Description(
    name = "DoubleSummarySketchToPercentile",
    value = "_FUNC_(sketch)",
    extended = "Returns a percentile from a given Sketch<DoubleSummary>."
  + " The values from DoubleSummary objects in the sketch are extracted,  and"
  + " a single value with a given normalized rank is returned. The rank is in"
  + " percent. For example, 50th percentile is the median value of the"
  + " distribution (the number separating the higher half of a probability"
  + " distribution from the lower half)")
public class DoubleSummarySketchToPercentileUDF extends UDF {

  private static final int QUANTILES_SKETCH_SIZE = 1024;

  /**
   * Get percentile from a given Sketch&lt;DoubleSummary&gt;
   * @param serializedSketch DoubleSummarySketch in as serialized binary
   * @param percentile normalized rank in percent
   * @return percentile value
   */
  public Double evaluate(final BytesWritable serializedSketch, final double percentile) {
    if (serializedSketch == null) { return null; }
    if ((percentile < 0) || (percentile > 100)) {
      throw new IllegalArgumentException("percentile must be between 0 and 100");
    }
    final Sketch<DoubleSummary> sketch =
        Sketches.heapifySketch(Memory.wrap(serializedSketch.getBytes()));
    final UpdateDoublesSketch qs = DoublesSketch.builder().setK(QUANTILES_SKETCH_SIZE).build();
    final SketchIterator<DoubleSummary> it = sketch.iterator();
    while (it.next()) {
      qs.update(it.getSummary().getValue());
    }
    return qs.getQuantile(percentile / 100);
  }

}
