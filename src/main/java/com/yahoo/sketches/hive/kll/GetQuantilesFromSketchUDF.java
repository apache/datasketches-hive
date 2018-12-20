/*
 * Copyright 2018, Oath Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.kll;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.kll.KllFloatsSketch;

@Description(
  name = "GetQuantiles",
  value = "_FUNC_(sketch, fractions...)",
  extended = "Returns quantile values from a given KllFloatsSketch based on a given list of fractions."
  + " The fractions represent normalized ranks, and must be from 0 to 1 inclusive."
  + " For example, a fraction of 0.5 corresponds to 50th percentile,"
  + " which is the median value of the distribution (the number separating the higher"
  + " half of the probability distribution from the lower half).")
public class GetQuantilesFromSketchUDF extends UDF {

  /**
   * Returns a list of quantile values from a given sketch
   * @param serializedSketch serialized sketch
   * @param fractions list of values from 0 to 1 inclusive
   * @return list of quantile values
   */
  public List<Float> evaluate(final BytesWritable serializedSketch, final Double... fractions) {
    if (serializedSketch == null) { return null; }
    final KllFloatsSketch sketch = KllFloatsSketch.heapify(Memory.wrap(serializedSketch.getBytes()));
    return Util.primitivesToList(sketch.getQuantiles(Util.objectsToPrimitives(fractions)));
  }

}
