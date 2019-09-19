/*
 * Copyright 2019, Verizon Media.
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
  name = "GetCDF",
  value = "_FUNC_(sketch, split points...)",
  extended = "Returns an approximation to the Cumulative Distribution Function (CDF)"
  + " from a sketch given a set of split points (values)."
  + " Split points are an array of M unique, monotonically increasing values"
  + " that divide the real number line into M+1 consecutive disjoint intervals."
  + " The function returns an array of M+1 double valuess, the first M of which are approximations"
  + " to the ranks of the corresponding split points (fraction of input stream values that are less"
  + " than a split point). The last value is always 1."
  + " CDF can also be viewed as a cumulative version of PMF.")
@SuppressWarnings("javadoc")
public class GetCdfUDF extends UDF {

  /**
   * Returns a list of ranks (CDF) from a given sketch
   * @param serializedSketch serialized sketch
   * @param splitPoints list of unique and monotonically increasing values
   * @return list of fractions from 0 to 1
   */
  public List<Double> evaluate(final BytesWritable serializedSketch, final Float... splitPoints) {
    if (serializedSketch == null) { return null; }
    final KllFloatsSketch sketch = KllFloatsSketch.heapify(Memory.wrap(serializedSketch.getBytes()));
    final double[] cdf = sketch.getCDF(Util.objectsToPrimitives(splitPoints));
    if (cdf == null) { return null; }
    return Util.primitivesToList(cdf);
  }

}
