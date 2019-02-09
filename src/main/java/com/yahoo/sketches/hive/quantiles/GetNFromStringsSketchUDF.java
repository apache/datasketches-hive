/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.quantiles;

import java.util.Comparator;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.quantiles.ItemsSketch;

@Description(name = "GetN", value = "_FUNC_(sketch)",
extended = " Returns the total number of observed input values (stream length) from a given ItemsSketch<String>.")
public class GetNFromStringsSketchUDF extends UDF {

  /**
   * Returns N from a given sketch
   * @param serializedSketch serialized sketch
   * @return stream length
   */
  public Long evaluate(final BytesWritable serializedSketch) {
    if (serializedSketch == null) { return null; }
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(
      Memory.wrap(serializedSketch.getBytes()),
      Comparator.naturalOrder(),
      new ArrayOfStringsSerDe()
    );
    return sketch.getN();
  }

}
