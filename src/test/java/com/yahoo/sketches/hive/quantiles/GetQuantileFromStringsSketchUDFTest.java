/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.quantiles;

import java.util.Comparator;

import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.ArrayOfLongsSerDe;
import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.quantiles.ItemsSketch;

@SuppressWarnings("javadoc")
public class GetQuantileFromStringsSketchUDFTest {

  static final Comparator<String> comparator = Comparator.naturalOrder();
  static final ArrayOfItemsSerDe<String> serDe = new ArrayOfStringsSerDe();

  @Test
  public void nullSketch() {
    String result = new GetQuantileFromStringsSketchUDF().evaluate(null, 0);
    Assert.assertNull(result);
  }

  @Test
  public void normalCase() {
    ItemsSketch<String> sketch = ItemsSketch.getInstance(comparator);
    sketch.update("a");
    sketch.update("b");
    sketch.update("c");
    String result = new GetQuantileFromStringsSketchUDF()
        .evaluate(new BytesWritable(sketch.toByteArray(serDe)), 0.5);
    Assert.assertNotNull(result);
    Assert.assertEquals(result, "b");
  }

  //Note: this exception is only caught because a bounds error was detected.
  //If a bounds error is not detected from a wrong type assignment, unexpected results could occur.
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void fractionsWrongSketchType() {
    ItemsSketch<Long> sketch = ItemsSketch.getInstance(Comparator.naturalOrder());
    sketch.update(1L);
    sketch.update(2L);
    sketch.update(3L);
    new GetQuantileFromStringsSketchUDF() //WRONG SKETCH
      .evaluate(new BytesWritable(sketch.toByteArray(new ArrayOfLongsSerDe())), 0.5);
  }

}
