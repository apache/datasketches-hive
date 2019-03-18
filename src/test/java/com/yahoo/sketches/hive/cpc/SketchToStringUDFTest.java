/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.cpc;

import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.cpc.CpcSketch;

public class SketchToStringUDFTest {

  @Test
  public void nullSketch() {
    final String result = new SketchToStringUDF().evaluate(null);
    Assert.assertNull(result);
  }

  @Test
  public void emptySketch() {
    final CpcSketch sketch = new CpcSketch();
    final String result = new SketchToStringUDF().evaluate(new BytesWritable(sketch.toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertTrue(result.length() > 0);
  }

  @Test
  public void normalCase() {
    final CpcSketch sketch = new CpcSketch();
    sketch.update(1);
    sketch.update(2);
    final String result = new SketchToStringUDF().evaluate(new BytesWritable(sketch.toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertTrue(result.length() > 0);
  }

  @Test
  public void customSeed() {
    final long seed = 123;
    final CpcSketch sketch = new CpcSketch(12, seed);
    sketch.update(1);
    sketch.update(2);
    final String result = new SketchToStringUDF().evaluate(new BytesWritable(sketch.toByteArray()), seed);
    Assert.assertNotNull(result);
    Assert.assertTrue(result.length() > 0);
  }

}
