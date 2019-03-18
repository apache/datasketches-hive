/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.cpc;

import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.cpc.CpcSketch;

public class GetEstimateAndErrorBoundsUDFTest {

  @Test
  public void nullSketch() {
    final List<Double> result = new GetEstimateAndErrorBoundsUDF().evaluate(null);
    Assert.assertNull(result);
  }

  @Test
  public void emptySketch() {
    final CpcSketch sketch = new CpcSketch(12);
    final List<Double> result = new GetEstimateAndErrorBoundsUDF().evaluate(new BytesWritable(sketch.toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 3);
    Assert.assertEquals(result.get(0), 0.0);
    Assert.assertEquals(result.get(1), 0.0);
    Assert.assertEquals(result.get(2), 0.0);
  }

  @Test
  public void nonEmptySketchDefaultParams() {
    final CpcSketch sketch = new CpcSketch(12);
    sketch.update(1);
    sketch.update(2);
    final List<Double> result = new GetEstimateAndErrorBoundsUDF().evaluate(new BytesWritable(sketch.toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(result.get(0), 2.0, 0.01);
    Assert.assertTrue(result.get(1) <= 2.0);
    Assert.assertTrue(result.get(2) >= 2.0);
  }

  @Test
  public void nonEmptySketchExplicitKappa() {
    final CpcSketch sketch = new CpcSketch(12);
    sketch.update(1);
    sketch.update(2);
    final List<Double> result = new GetEstimateAndErrorBoundsUDF().evaluate(new BytesWritable(sketch.toByteArray()), 2);
    Assert.assertNotNull(result);
    Assert.assertEquals(result.get(0), 2.0, 0.01);
    Assert.assertTrue(result.get(1) <= 2.0);
    Assert.assertTrue(result.get(2) >= 2.0);
  }

  @Test
  public void customSeed() {
    final long seed = 123;
    final CpcSketch sketch = new CpcSketch(12, seed);
    sketch.update(1);
    sketch.update(2);
    final List<Double> result = new GetEstimateAndErrorBoundsUDF().evaluate(new BytesWritable(sketch.toByteArray()), 1, seed);
    Assert.assertNotNull(result);
    Assert.assertEquals(result.get(0), 2.0, 0.01);
    Assert.assertTrue(result.get(1) <= 2.0);
    Assert.assertTrue(result.get(2) >= 2.0);
  }

}
