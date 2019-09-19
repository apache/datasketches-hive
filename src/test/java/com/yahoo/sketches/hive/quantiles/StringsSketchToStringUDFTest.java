/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.quantiles;

import java.util.Comparator;

import org.apache.hadoop.io.BytesWritable;

import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.quantiles.ItemsSketch;

import org.testng.annotations.Test;
import org.testng.Assert;

@SuppressWarnings("javadoc")
public class StringsSketchToStringUDFTest {

  static final Comparator<String> COMPARATOR = Comparator.naturalOrder();
  static final ArrayOfItemsSerDe<String> SERDE = new ArrayOfStringsSerDe();

  @Test
  public void nullSketch() {
    final String result = new StringsSketchToStringUDF().evaluate(null);
    Assert.assertNull(result);
  }

  @Test
  public void normalCase() {
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(COMPARATOR);
    final String result = new StringsSketchToStringUDF().evaluate(new BytesWritable(sketch.toByteArray(SERDE)));
    Assert.assertNotNull(result);
    Assert.assertTrue(result.length() > 0);
  }

}
