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

  private static final int EMPTY_SKETCH_SIZE_BYTES = 8;

  /**
   * Main logic called by hive, calculates the estimate unique count of sketch.
   * 
   * @param binarySketch
   *           sketch to be estimated passed in as bytes writable.
   * @return the estimate of unique count from given sketch.
   */
  public Double evaluate(final BytesWritable binarySketch) {
    if (binarySketch == null) {
      return 0.0;
    }

    final byte[] serializedSketch = binarySketch.getBytes();

    if (serializedSketch.length <= EMPTY_SKETCH_SIZE_BYTES) {
      return 0.0;
    }

    return Sketch.wrap(new NativeMemory(serializedSketch)).getEstimate();
  }
}
