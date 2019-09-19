/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.hive.quantiles;

import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.ArrayOfLongsSerDe;
import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.quantiles.ItemsSketch;

@SuppressWarnings("javadoc")
public class GetQuantilesFromStringsSketchUDFTest {

  static final Comparator<String> comparator = Comparator.naturalOrder();
  static final ArrayOfItemsSerDe<String> serDe = new ArrayOfStringsSerDe();

  @Test
  public void nullSketch() {
    List<String> result = new GetQuantilesFromStringsSketchUDF().evaluate(null, 0.0);
    Assert.assertNull(result);
  }

  @Test
  public void emptyListOfFractions() {
    ItemsSketch<String> sketch = ItemsSketch.getInstance(comparator);
    sketch.update("a");
    sketch.update("b");
    sketch.update("c");
    List<String> result = new GetQuantilesFromStringsSketchUDF().evaluate(new BytesWritable(sketch.toByteArray(serDe)));
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 0);
  }

  @Test
  public void fractionsNormalCase() {
    ItemsSketch<String> sketch = ItemsSketch.getInstance(comparator);
    sketch.update("a");
    sketch.update("b");
    sketch.update("c");
    List<String> result = new GetQuantilesFromStringsSketchUDF().evaluate(new BytesWritable(sketch.toByteArray(serDe)), 0.0, 0.5, 1.0);
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 3);
    Assert.assertEquals(result.get(0), "a");
    Assert.assertEquals(result.get(1), "b");
    Assert.assertEquals(result.get(2), "c");
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void evenlySpacedZero() {
    ItemsSketch<String> sketch = ItemsSketch.getInstance(comparator);
    sketch.update("a");
    new GetQuantilesFromStringsSketchUDF()
      .evaluate(new BytesWritable(sketch.toByteArray(serDe)), 0);
  }

  @Test
  public void evenlySpacedEmptySketch() {
    ItemsSketch<String> sketch = ItemsSketch.getInstance(comparator);
    List<String> result = new GetQuantilesFromStringsSketchUDF().evaluate(new BytesWritable(sketch.toByteArray(serDe)), 1);
    Assert.assertNull(result);
  }

  @Test
  public void evenlySpacedNormalCase() {
    ItemsSketch<String> sketch = ItemsSketch.getInstance(comparator);
    sketch.update("a");
    sketch.update("b");
    sketch.update("c");
    List<String> result = new GetQuantilesFromStringsSketchUDF().evaluate(new BytesWritable(sketch.toByteArray(serDe)), 3);
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 3);
    Assert.assertEquals(result.get(0), "a");
    Assert.assertEquals(result.get(1), "b");
    Assert.assertEquals(result.get(2), "c");
  }

  //Note: this exception is only caught because a bounds error was detected.
  //If a bounds error is not detected from a wrong type assignment, unexpected results could occur.
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void fractionsWrongSketchType() {
    ItemsSketch<Long> sketch = ItemsSketch.getInstance(Comparator.naturalOrder());
    sketch.update(1L);
    sketch.update(2L);
    sketch.update(3L);
    new GetQuantilesFromStringsSketchUDF() //WRONG SKETCH
      .evaluate(new BytesWritable(sketch.toByteArray(new ArrayOfLongsSerDe())), 0.5);
  }

  //Note: this exception is only caught because a bounds error was detected.
  //If a bounds error is not detected from a wrong type assignment, unexpected results could occur.
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void evenlySpacedWrongSketchType() {
    ItemsSketch<Long> sketch = ItemsSketch.getInstance(Comparator.naturalOrder());
    sketch.update(1L);
    sketch.update(2L);
    sketch.update(3L);
    new GetQuantilesFromStringsSketchUDF() //WRONG SKETCH
      .evaluate(new BytesWritable(sketch.toByteArray(new ArrayOfLongsSerDe())), 1);
  }

}
