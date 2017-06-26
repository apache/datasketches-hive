/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.hive.quantiles;

import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.UpdateDoublesSketch;

public class GetPmfFromDoublesSketchUDFTest {

  @Test
  public void nullSketch() {
    List<Double> result = new GetPmfFromDoublesSketchUDF().evaluate(null, 0.0);
    Assert.assertNull(result);
  }

  @Test
  public void emptyListOfSplitPoints() {
    UpdateDoublesSketch sketch = DoublesSketch.builder().build();
    sketch.update(1);
    sketch.update(2);
    sketch.update(3);
    List<Double> result = new GetPmfFromDoublesSketchUDF().evaluate(new BytesWritable(sketch.toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), 1.0);
  }

  @Test
  public void normalCase() {
    UpdateDoublesSketch sketch = DoublesSketch.builder().build();
    sketch.update(1);
    sketch.update(2);
    sketch.update(3);
    sketch.update(4);
    List<Double> result = new GetPmfFromDoublesSketchUDF().evaluate(new BytesWritable(sketch.toByteArray()), 1.0, 3.0, 5.0);
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 4);
    Assert.assertEquals(result.get(0), 0.0);
    Assert.assertEquals(result.get(1), 0.5);
    Assert.assertEquals(result.get(2), 0.5);
    Assert.assertEquals(result.get(3), 0.0);
  }

}
