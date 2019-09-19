/*******************************************************************************
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 *******************************************************************************/
package com.yahoo.sketches.hive.theta;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static org.testng.AssertJUnit.assertEquals;

import org.apache.hadoop.io.BytesWritable;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.UpdateSketch;

@SuppressWarnings("javadoc")
public class ExcludeSketchUDFTest {

  @Test
  public void evaluateNull() {
    ExcludeSketchUDF testObject = new ExcludeSketchUDF();

    BytesWritable intermResult = testObject.evaluate(null, null);

    Memory mem = Memory.wrap(intermResult.getBytes());

    Sketch testResult = Sketches.wrapSketch(mem);

    assertEquals(0.0, testResult.getEstimate());
  }

  @Test
  public void evaluateEmpty() {
    ExcludeSketchUDF testObject = new ExcludeSketchUDF();

    BytesWritable intermResult = testObject.evaluate(new BytesWritable(), new BytesWritable());

    Memory mem = Memory.wrap(intermResult.getBytes());

    Sketch testResult = Sketches.wrapSketch(mem);

    assertEquals(0.0, testResult.getEstimate());
  }

  @Test
  public void evaluateValidSketch () {
    ExcludeSketchUDF testObject = new ExcludeSketchUDF();

    UpdateSketch sketch1 = Sketches.updateSketchBuilder().setNominalEntries(1024).build();
    for (int i = 0; i<128; i++) {
      sketch1.update(i);
    }

    UpdateSketch sketch2 = Sketches.updateSketchBuilder().setNominalEntries(1024).build();
    for (int i = 100; i<128; i++) {
      sketch2.update(i);
    }

    BytesWritable input1 = new BytesWritable(sketch1.compact(true, null).toByteArray());
    BytesWritable input2 = new BytesWritable(sketch2.compact(true, null).toByteArray());

    BytesWritable output = testObject.evaluate(input1, input2);

    Sketch result = Sketches.wrapSketch(Memory.wrap(output.getBytes()));

    assertEquals(100.0, result.getEstimate());
  }

  @Test
  public void evaluateValidSketchWithDefaultSeed () {
    ExcludeSketchUDF testObject = new ExcludeSketchUDF();

    UpdateSketch sketch1 = Sketches.updateSketchBuilder().setNominalEntries(1024).build();
    for (int i = 0; i<128; i++) {
      sketch1.update(i);
    }

    UpdateSketch sketch2 = Sketches.updateSketchBuilder().setNominalEntries(1024).build();
    for (int i = 100; i<128; i++) {
      sketch2.update(i);
    }

    BytesWritable input1 = new BytesWritable(sketch1.compact(true, null).toByteArray());
    BytesWritable input2 = new BytesWritable(sketch2.compact(true, null).toByteArray());

    BytesWritable output = testObject.evaluate(input1, input2, DEFAULT_UPDATE_SEED);

    Sketch result = Sketches.wrapSketch(Memory.wrap(output.getBytes()));

    assertEquals(100.0, result.getEstimate());
  }

  @Test
  public void evaluateValidSketchWithCustomSeed () {
    ExcludeSketchUDF testObject = new ExcludeSketchUDF();

    final long seed = 1;
    UpdateSketch sketch1 = Sketches.updateSketchBuilder().setSeed(seed).setNominalEntries(1024).build();
    for (int i = 0; i<128; i++) {
      sketch1.update(i);
    }

    UpdateSketch sketch2 = Sketches.updateSketchBuilder().setSeed(seed).setNominalEntries(1024).build();
    for (int i = 100; i<128; i++) {
      sketch2.update(i);
    }

    BytesWritable input1 = new BytesWritable(sketch1.compact(true, null).toByteArray());
    BytesWritable input2 = new BytesWritable(sketch2.compact(true, null).toByteArray());

    BytesWritable output = testObject.evaluate(input1, input2, seed);

    Sketch result = Sketches.wrapSketch(Memory.wrap(output.getBytes()), seed);

    assertEquals(100.0, result.getEstimate());
  }

}
