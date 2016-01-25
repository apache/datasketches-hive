/*******************************************************************************
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 *******************************************************************************/
package com.yahoo.sketches.hive.theta;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;

import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.AnotB;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;

/**
 * Hive exclude sketch UDF. (i.e. in sketch a but not in sketch b)
 *
 */
public class ExcludeSketchUDF extends UDF {
  //public static final int DEFAULT_SIZE = 16384;

  /**
   * Main logic called by hive if sketchSize is also passed in. Computes the
   * hash in first sketch excluding the hash in second sketch of two sketches of
   * same or different column.
   * 
   * @param firstSketch
   *          first sketch to be included.
   * @param secondSketch
   *          second sketch to be excluded.
   * @param hashUpdateSeed
   *          Only required if input sketches were constructed using an update seed that was not the default.
   * @return resulting sketch of exclusion.
   */
  public BytesWritable evaluate(BytesWritable firstSketch, BytesWritable secondSketch, LongWritable hashUpdateSeed) {
    long hashSeed = (hashUpdateSeed == null)? DEFAULT_UPDATE_SEED : hashUpdateSeed.get();

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

    AnotB anotb = SetOperation.builder().setSeed(hashSeed).buildANotB();

    anotb.update(firstHeapSketch, secondHeapSketch);

    Sketch excludeSketch = anotb.getResult(true, null);

    byte[] resultSketch = excludeSketch.toByteArray();

    BytesWritable result = new BytesWritable();
    result.set(resultSketch, 0, resultSketch.length);

    return result;
  }

  /**
   * Main logic called by hive if hashUpdateSeed is not passed in. Computes the hash
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
