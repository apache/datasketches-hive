/*******************************************************************************
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 *******************************************************************************/
package com.yahoo.sketches.hive.theta;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;

import com.yahoo.sketches.theta.SetOpReturnState;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.AnotB;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;

/**
 * Hive exclude sketch UDF. (i.e. in sketch a but not in sketch b)
 *
 */
public class ExcludeSketchUDF extends UDF {
  public static final int DEFAULT_SIZE = 16384;

  /**
   * Main logic called by hive if sketchSize is also passed in. Computes the
   * hash in first sketch excluding the hash in second sketch of two sketches of
   * same or different column.
   * 
   * @param firstSketch
   *          first sketch to be included.
   * @param secondSketch
   *          second sketch to be excluded.
   * @param sketchSize
   *          final output excluded sketch size.
   * @return resulting sketch of exclusion.
   */
  public BytesWritable evaluate(BytesWritable firstSketch, BytesWritable secondSketch, IntWritable sketchSize) {
    int sketch_size = DEFAULT_SIZE;

    Sketch firstHeapSketch, secondHeapSketch;

    // if first sketch is null, throw exception
    if (firstSketch == null || firstSketch.getLength() == 0) {
      firstHeapSketch = null;
    } else {
      NativeMemory firstMemory = new NativeMemory(firstSketch.getBytes());
      firstHeapSketch = Sketch.heapify(firstMemory);
    }

    // if second sketch is null, continue with second sketch as empty sketch
    if (secondSketch == null || secondSketch.getLength() == 0) {
      secondHeapSketch = null;
    } else {
      NativeMemory secondMemory = new NativeMemory(secondSketch.getBytes());
      secondHeapSketch = Sketch.heapify(secondMemory);
    }

    if (sketchSize != null) {
      sketch_size = sketchSize.get();
    }

    AnotB anotb = (AnotB) SetOperation.builder().build(sketch_size, Family.A_NOT_B);

    SetOpReturnState success = anotb.update(firstHeapSketch, secondHeapSketch);

    if (success != SetOpReturnState.Success) {
      throw new IllegalStateException("HiveSketchError: Exclude sketch operation failed.");
    }

    Sketch excludeSketch = anotb.getResult(false, null);

    byte[] resultSketch = excludeSketch.toByteArray();

    BytesWritable result = new BytesWritable();
    result.set(resultSketch, 0, resultSketch.length);

    return result;
  }

  /**
   * Main logic called by hive if sketchSize is not passed in. Computes the hash
   * in first sketch excluding the hash in second sketch of two sketches of same
   * or different column.
   * 
   * @param firstSketch
   *          first sketch to be included.
   * @param secondSketch
   *          second sketch to be excluded.
   * @return resulting sketch of exclusion.
   */
  public BytesWritable evaluate(BytesWritable firstSketch, BytesWritable secondSketch) {

    return evaluate(firstSketch, secondSketch, null);

  }
}
