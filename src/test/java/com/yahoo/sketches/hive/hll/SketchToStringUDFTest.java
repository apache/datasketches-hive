/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.hll;

import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.hll.HllSketch;

public class SketchToStringUDFTest {

  @Test
  public void nullSketch() {
    final String result = new SketchToStringUDF().evaluate(null);
    Assert.assertNull(result);
  }

  @Test
  public void emptySketch() {
    final HllSketch sketch = new HllSketch(12);
    final String result = new SketchToStringUDF().evaluate(new BytesWritable(sketch.toCompactByteArray()));
    Assert.assertNotNull(result);
    Assert.assertTrue(result.length() > 0);
  }

  @Test
  public void normalCase() {
    final HllSketch sketch = new HllSketch(12);
    sketch.update(1);
    sketch.update(2);
    final String result = new SketchToStringUDF().evaluate(new BytesWritable(sketch.toCompactByteArray()));
    Assert.assertNotNull(result);
    Assert.assertTrue(result.length() > 0);
  }

}
