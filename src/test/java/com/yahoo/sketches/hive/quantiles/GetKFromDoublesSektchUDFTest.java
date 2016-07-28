/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.hive.quantiles;

import com.yahoo.sketches.quantiles.DoublesSketch;

import org.testng.annotations.Test;
import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;

public class GetKFromDoublesSektchUDFTest {

  @Test
  public void nullSketch() {
    Integer result = new GetKFromDoublesSketchUDF().evaluate(null);
    Assert.assertNull(result);
  }

  @Test
  public void normalCase() {
    DoublesSketch sketch = DoublesSketch.builder().build();
    Integer result = new GetKFromDoublesSketchUDF().evaluate(new BytesWritable(sketch.toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(result, Integer.valueOf(128));
  }

}
