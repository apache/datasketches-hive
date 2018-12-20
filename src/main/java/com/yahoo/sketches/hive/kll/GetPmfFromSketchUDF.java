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
  name = "GetPMF",
  value = "_FUNC_(sketch, split points...)",
  extended = "Returns an approximation to the Probability Mass Function (PMF)"
  + " from a sketch given a set of split points (values)."
  + " Split points are an array of M unique, monotonically increasing values"
  + " that divide the real number line into M+1 consecutive disjoint intervals."
  + " The function returns an array of M+1 doubles, each of which is an approximation"
  + " to the fraction of the values that fell into one of those intervals."
  + " The definition of an interval is inclusive of the left split point and exclusive"
  + " of the right split point")
public class GetPmfFromSketchUDF extends UDF {

  /**
   * Returns a list of fractions (PMF) from a given sketch
   * @param serializedSketch serialized sketch
   * @param splitPoints list of unique and monotonically increasing values
   * @return list of fractions from 0 to 1
   */
  public List<Double> evaluate(final BytesWritable serializedSketch, final Float... splitPoints) {
    if (serializedSketch == null) { return null; }
    final KllFloatsSketch sketch = KllFloatsSketch.heapify(Memory.wrap(serializedSketch.getBytes()));
    final double[] pmf = sketch.getPMF(Util.objectsToPrimitives(splitPoints));
    if (pmf == null) { return null; }
    return Util.primitivesToList(pmf);
  }

}
