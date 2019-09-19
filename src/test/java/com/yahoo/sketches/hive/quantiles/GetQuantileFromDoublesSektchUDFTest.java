/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.hive.quantiles;

import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.UpdateDoublesSketch;

@SuppressWarnings("javadoc")
public class GetQuantileFromDoublesSektchUDFTest {

  @Test
  public void nullSketch() {
    Double result = new GetQuantileFromDoublesSketchUDF().evaluate(null, 0);
    Assert.assertNull(result);
  }

  @Test
  public void normalCase() {
    UpdateDoublesSketch sketch = DoublesSketch.builder().build();
    sketch.update(1);
    sketch.update(2);
    sketch.update(3);
    Double result = new GetQuantileFromDoublesSketchUDF().evaluate(new BytesWritable(sketch.toByteArray()), 0.5);
    Assert.assertNotNull(result);
    Assert.assertEquals(result, 2.0);
  }

}
