/*******************************************************************************
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 *******************************************************************************/
package com.yahoo.sketches.hive.theta;

import org.apache.hadoop.io.BytesWritable;
import org.testng.annotations.Test;

import com.yahoo.sketches.hive.theta.EstimateSketchUDF;

import com.yahoo.sketches.theta.CompactSketch;
import com.yahoo.sketches.theta.UpdateSketch;
import com.yahoo.sketches.theta.Sketches;
import static org.testng.AssertJUnit.assertEquals;

public class EstimateSketchUDFTest {

  // test null input
  @Test
  public void testEvaluateNull() {
    EstimateSketchUDF testObject = new EstimateSketchUDF();

    Double testResult = testObject.evaluate(null);

    assertEquals(testResult, 0.0);
  }

  // test empty input
  @Test
  public void testEvaluateEmptyInput() {
    EstimateSketchUDF testObject = new EstimateSketchUDF();

    BytesWritable testInput = new BytesWritable();

    Double testResult = testObject.evaluate(testInput);

    assertEquals(testResult, 0.0);
  }
  
  // test valid input
  @Test
  public void testEvaluateValid() {
    EstimateSketchUDF testObject = new EstimateSketchUDF();
    
    UpdateSketch sketch = Sketches.updateSketchBuilder().build(1024);
    for (int i = 0; i<128; i++) {
      sketch.update(i);
    }
    
    BytesWritable input = new BytesWritable(sketch.toByteArray());
    
    Double testResult = testObject.evaluate(input);
    
    assertEquals(128.0, testResult);
    
    CompactSketch compactSketch = sketch.compact(false, null);
    input = new BytesWritable(compactSketch.toByteArray());
    
    testResult = testObject.evaluate(input);
    
    assertEquals(128.0, testResult);
  }

}
