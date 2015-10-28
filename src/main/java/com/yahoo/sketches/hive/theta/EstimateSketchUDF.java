/*******************************************************************************
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 *******************************************************************************/
package com.yahoo.sketches.hive.theta;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.Sketch;

/**
 * Hive estimate sketch udf. V4
 *
 */
public class EstimateSketchUDF extends UDF {

  /**
   * Main logic called by hive, calculates the estimate unique count of sketch.
   * 
   * @param binarySketch
   *           sketch to be estimated passed in as bytes writable.
   * @return the estimate of unique count from given sketch.
   */
  public Double evaluate(BytesWritable binarySketch) {
    if (binarySketch == null) {
      return 0.0;
    }

    byte[] serializedSketch = binarySketch.getBytes();

    if (serializedSketch.length <= 8) {
      return 0.0;
    }

    NativeMemory memorySketch = new NativeMemory(serializedSketch);

    Sketch heapSketch = Sketch.heapify(memorySketch);

    return heapSketch.getEstimate();
  }
}
