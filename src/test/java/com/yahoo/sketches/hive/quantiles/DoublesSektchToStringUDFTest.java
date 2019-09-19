/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.quantiles;

import com.yahoo.sketches.quantiles.DoublesSketch;

import org.testng.annotations.Test;
import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;

@SuppressWarnings("javadoc")
public class DoublesSektchToStringUDFTest {

  @Test
  public void nullSketch() {
    final String result = new DoublesSketchToStringUDF().evaluate(null);
    Assert.assertNull(result);
  }

  @Test
  public void normalCase() {
    final DoublesSketch sketch = DoublesSketch.builder().build();
    final String result = new DoublesSketchToStringUDF().evaluate(new BytesWritable(sketch.toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertTrue(result.length() > 0);
  }

}
