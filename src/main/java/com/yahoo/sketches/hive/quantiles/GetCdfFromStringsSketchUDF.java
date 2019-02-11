/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.quantiles;

import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.quantiles.ItemsSketch;

@Description(
    name = "GetCDF",
    value = "_FUNC_(sketch, split points...)",
    extended = "Returns an approximation to the Cumulative Distribution Function (CDF)"
    + " from a sketch given a set of split points (values)."
    + " Split points are an array of M unique, monotonically increasing values"
    + " that divide the domain into M+1 consecutive disjoint intervals."
    + " The function returns an array of M+1 double valuess, the first M of which are approximations"
    + " to the ranks of the corresponding split points (fraction of input stream values that are less"
    + " than a split point). The last value is always 1."
    + " CDF can also be viewed as a cumulative version of PMF.")
public class GetCdfFromStringsSketchUDF extends UDF {

  /**
   * Returns a list of ranks (CDF) from a given sketch
   * @param serializedSketch serialized sketch
   * @param splitPoints list of unique and monotonically increasing values
   * @return list of fractions from 0 to 1
   */
  public List<Double> evaluate(final BytesWritable serializedSketch, final String... splitPoints) {
    if (serializedSketch == null) { return null; }
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(
      Memory.wrap(serializedSketch.getBytes()),
      Comparator.naturalOrder(),
      new ArrayOfStringsSerDe()
    );
    final double[] cdf = sketch.getCDF(splitPoints);
    if (cdf == null) { return null; }
    return Util.primitivesToList(cdf);
  }

}
