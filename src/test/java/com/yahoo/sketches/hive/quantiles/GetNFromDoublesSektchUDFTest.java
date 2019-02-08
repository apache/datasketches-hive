/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.quantiles;

import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.UpdateDoublesSketch;

import org.testng.annotations.Test;
import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;

public class GetNFromDoublesSektchUDFTest {

  @Test
  public void nullSketch() {
    Long result = new GetNFromDoublesSketchUDF().evaluate(null);
    Assert.assertNull(result);
  }

  @Test
  public void normalCase() {
    UpdateDoublesSketch sketch = DoublesSketch.builder().build();
    sketch.update(1);
    sketch.update(2);
    sketch.update(3);
    Long result = new GetNFromDoublesSketchUDF().evaluate(new BytesWritable(sketch.toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(result, Long.valueOf(3));
  }

}
