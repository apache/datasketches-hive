/*
 * Copyright 2018, Oath Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.kll;

import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.kll.KllFloatsSketch;

public class GetPmfFromSketchUDFTest {

  @Test
  public void nullSketch() {
    List<Double> result = new GetPmfFromSketchUDF().evaluate(null, 0f);
    Assert.assertNull(result);
  }

  @Test
  public void emptyListOfSplitPoints() {
    KllFloatsSketch sketch = new KllFloatsSketch();
    sketch.update(1);
    sketch.update(2);
    sketch.update(3);
    List<Double> result = new GetPmfFromSketchUDF().evaluate(new BytesWritable(sketch.toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), 1.0);
  }

  @Test
  public void emptySketch() {
    KllFloatsSketch sketch = new KllFloatsSketch();
    List<Double> result = new GetPmfFromSketchUDF().evaluate(new BytesWritable(sketch.toByteArray()), 0f);
    Assert.assertNull(result);
  }

  @Test
  public void normalCase() {
    KllFloatsSketch sketch = new KllFloatsSketch();
    sketch.update(1);
    sketch.update(2);
    sketch.update(3);
    sketch.update(4);
    List<Double> result = new GetPmfFromSketchUDF().evaluate(new BytesWritable(sketch.toByteArray()), 1f, 3f, 5f);
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 4);
    Assert.assertEquals(result.get(0), 0.0);
    Assert.assertEquals(result.get(1), 0.5);
    Assert.assertEquals(result.get(2), 0.5);
    Assert.assertEquals(result.get(3), 0.0);
  }

}
