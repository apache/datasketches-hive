/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.quantiles;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.quantiles.ItemsSketch;

@Description(
    name = "GetQuantiles",
    value = "_FUNC_(sketch, fractions...) or _FUNC_(sketch, number)",
    extended = "Returns quantile values from a given ItemsSketch<String> based on a given"
    + " list of fractions or a number of evenly spaced fractions."
    + " The fractions represent normalized ranks, and must be from 0 to 1 inclusive."
    + " For example, a fraction of 0.5 corresponds to 50th percentile,"
    + " which is the median value of the distribution (the number separating the higher"
    + " half of the probability distribution from the lower half)."
    + " The number of evenly spaced fractions must be a positive integer greater than 0."
    + " A value of 1 will return the min value (normalized rank of 0.0)."
    + " A value of 2 will return the min and the max value (ranks 0.0 amd 1.0)."
    + " A value of 3 will return the min, the median and the max value (ranks 0.0, 0.5, and 1.0), etc.")
public class GetQuantilesFromStringsSketchUDF extends UDF {

  /**
   * Returns a list of quantile values from a given sketch
   * @param serializedSketch serialized sketch
   * @param fractions list of values from 0 to 1 inclusive
   * @return list of quantile values
   */
  public List<String> evaluate(final BytesWritable serializedSketch, final Double... fractions) {
    if (serializedSketch == null) { return null; }
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(
      Memory.wrap(serializedSketch.getBytes()),
      Comparator.naturalOrder(),
      new ArrayOfStringsSerDe()
    );
    return Arrays.asList(sketch.getQuantiles(Util.objectsToPrimitives(fractions)));
  }

  /**
   * Returns a list of quantile values from a given sketch
   * @param serializedSketch serialized sketch
   * @param number of evenly spaced fractions
   * @return list of quantile values
   */
  public List<String> evaluate(final BytesWritable serializedSketch, final int number) {
    if (serializedSketch == null) { return null; }
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(
      Memory.wrap(serializedSketch.getBytes()),
      Comparator.naturalOrder(),
      new ArrayOfStringsSerDe()
    );
    return Arrays.asList(sketch.getQuantiles(number));
  }

}
