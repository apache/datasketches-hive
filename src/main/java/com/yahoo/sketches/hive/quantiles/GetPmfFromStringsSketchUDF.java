/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.quantiles;

import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.quantiles.ItemsSketch;

@Description(
    name = "GetPMF",
    value = "_FUNC_(sketch, split points...)",
    extended = "Returns an approximation to the Probability Mass Function (PMF)"
    + " from a sketch given a set of split points (values)."
    + " Split points are an array of M unique, monotonically increasing values"
    + " that divide the domain into M+1 consecutive disjoint intervals."
    + " The function returns an array of M+1 doubles, each of which is an approximation"
    + " to the fraction of the values that fell into one of those intervals."
    + " The definition of an interval is inclusive of the left split point and exclusive"
    + " of the right split point")
public class GetPmfFromStringsSketchUDF extends UDF {

  /**
   * Returns a list of fractions (PMF) from a given sketch
   * @param serializedSketch serialized sketch
   * @param splitPoints list of unique and monotonically increasing values
   * @return list of fractions from 0 to 1
   */
  public List<Double> evaluate(final BytesWritable serializedSketch, final String... splitPoints) {
    if (serializedSketch == null) return null;
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(
      new NativeMemory(serializedSketch.getBytes()),
      Comparator.naturalOrder(),
      new ArrayOfStringsSerDe()
    );
    return Util.primitivesToList(sketch.getPMF(splitPoints));
  }

}
