/*******************************************************************************
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 *******************************************************************************/
package com.yahoo.sketches.hive.theta;

import static org.testng.AssertJUnit.assertEquals;

import org.apache.hadoop.io.BytesWritable;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.UpdateSketch;

public class IntersectSketchUDFTest  {

  @Test
  public void evaluateNull() {
    IntersectSketchUDF testObject = new IntersectSketchUDF();
    BytesWritable intermResult = testObject.evaluate(null, null);
    Memory mem = new NativeMemory(intermResult.getBytes());
    Sketch testResult = Sketches.heapifySketch(mem);
    assertEquals(0.0, testResult.getEstimate());
  }

  @Test
  public void evaluateEmpty() {
    IntersectSketchUDF testObject = new IntersectSketchUDF();
    BytesWritable intermResult = testObject.evaluate(new BytesWritable(), new BytesWritable());
    Memory mem = new NativeMemory(intermResult.getBytes());
    Sketch testResult = Sketches.heapifySketch(mem);
    assertEquals(0.0, testResult.getEstimate());
  }

  @Test
  public void evaluateValidSketch() {
    IntersectSketchUDF testObject = new IntersectSketchUDF();

    UpdateSketch sketch1 = Sketches.updateSketchBuilder().build(1024);
    for (int i = 0; i<128; i++) {
      sketch1.update(i);
    }

    UpdateSketch sketch2 = Sketches.updateSketchBuilder().build(1024);
    for (int i = 100; i<128; i++) {
      sketch2.update(i);
    }

    BytesWritable input1 = new BytesWritable(sketch1.compact().toByteArray());
    BytesWritable input2 = new BytesWritable(sketch2.compact().toByteArray());

    BytesWritable output = testObject.evaluate(input1, input2);

    Sketch result = Sketches.heapifySketch(new NativeMemory(output.getBytes()));

    assertEquals(28.0, result.getEstimate());
  }

  @Test
  public void evaluateValidSketchExpicitSeed() {
    IntersectSketchUDF testObject = new IntersectSketchUDF();

    final long seed = 1;
    UpdateSketch sketch1 = Sketches.updateSketchBuilder().setSeed(seed).build(1024);
    for (int i = 0; i<128; i++) {
      sketch1.update(i);
    }

    UpdateSketch sketch2 = Sketches.updateSketchBuilder().setSeed(seed).build(1024);
    for (int i = 100; i<128; i++) {
      sketch2.update(i);
    }

    BytesWritable input1 = new BytesWritable(sketch1.compact().toByteArray());
    BytesWritable input2 = new BytesWritable(sketch2.compact().toByteArray());

    BytesWritable output = testObject.evaluate(input1, input2, seed);

    Sketch result = Sketches.heapifySketch(new NativeMemory(output.getBytes()), seed);

    assertEquals(28.0, result.getEstimate());
  }

}
