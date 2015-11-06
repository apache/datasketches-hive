/*******************************************************************************
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 *******************************************************************************/
package com.yahoo.sketches.hive.theta;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Union;

/**
 * Hive union sketch UDF.
 *
 */
public class UnionSketchUDF extends UDF {
  public static final int DEFAULT_SIZE = 16384;

  /**
   * Main logic called by hive if sketchSize is also passed in. Union two
   * sketches of same or different column.
   * 
   * @param firstSketch
   *          first sketch to be unioned.
   * @param secondSketch
   *          second sketch to be unioned.
   * @param sketchSize
   *          final output unioned sketch size.
   * @return resulting sketch of union.
   */
  public BytesWritable evaluate(BytesWritable firstSketch, BytesWritable secondSketch, IntWritable sketchSize) {
    int sketch_size = DEFAULT_SIZE;
    Sketch firstHeapSketch, secondHeapSketch;

    if (sketchSize != null) {
      sketch_size = sketchSize.get();
    }

    Union union = (Union) SetOperation.builder().build(sketch_size, Family.UNION);

    // update union first sketch, if null do nothing
    if (firstSketch != null && firstSketch.getLength() > 0) {
      NativeMemory firstMemory = new NativeMemory(firstSketch.getBytes());
      firstHeapSketch = Sketch.heapify(firstMemory);
      union.update(firstHeapSketch);
    }

    // update union second sketch, if null do nothing
    if (secondSketch != null && secondSketch.getLength() > 0) {
      NativeMemory secondMemory = new NativeMemory(secondSketch.getBytes());
      secondHeapSketch = Sketch.heapify(secondMemory);
      union.update(secondHeapSketch);

    }

    Sketch intermediateSketch = union.getResult(false, null);
    byte[] resultSketch = intermediateSketch.toByteArray();

    BytesWritable result = new BytesWritable();
    result.set(resultSketch, 0, resultSketch.length);

    return result;
  }

  /**
   * Main logic called by hive if sketchSize is not passed in. Union two
   * sketches of same or different column.
   * 
   * @param firstSketch
   *          first sketch to be unioned.
   * @param secondSketch
   *          second sketch to be unioned.
   * @return resulting sketch of union.
   */
  public BytesWritable evaluate(BytesWritable firstSketch, BytesWritable secondSketch) {

    return evaluate(firstSketch, secondSketch, null);
  }

}
