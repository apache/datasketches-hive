/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.hive.quantiles;

import java.util.Comparator;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.quantiles.ItemsSketch;

@Description(name = "GetK", value = "_FUNC_(sketch)",
extended = " Returns parameter K from a given ItemsSketch<String> sketch.")
public class GetKFromStringsSketchUDF extends UDF {

  public Integer evaluate(final BytesWritable serializedSketch) {
    if (serializedSketch == null) return null;
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(
      new NativeMemory(serializedSketch.getBytes()),
      Comparator.naturalOrder(),
      new ArrayOfStringsSerDe()
    );
    return sketch.getK();
  }

}
