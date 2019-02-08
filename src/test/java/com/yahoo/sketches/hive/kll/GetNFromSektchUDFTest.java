/*
 * Copyright 2018, Oath Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.kll;

import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.kll.KllFloatsSketch;

public class GetNFromSektchUDFTest {

  @Test
  public void nullSketch() {
    final Long result = new GetNFromSketchUDF().evaluate(null);
    Assert.assertNull(result);
  }

  @Test
  public void normalCase() {
    KllFloatsSketch sketch = new KllFloatsSketch();
    sketch.update(1);
    sketch.update(2);
    sketch.update(3);
    sketch.update(4);
    final Long result = new GetNFromSketchUDF().evaluate(new BytesWritable(sketch.toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(result, Long.valueOf(4));
  }

}
