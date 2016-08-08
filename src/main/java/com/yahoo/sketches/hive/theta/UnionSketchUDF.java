/*******************************************************************************
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 *******************************************************************************/

package com.yahoo.sketches.hive.theta;

import static com.yahoo.sketches.Util.DEFAULT_NOMINAL_ENTRIES;
import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Union;

/**
 * Hive union sketch UDF.
 */
public class UnionSketchUDF extends UDF {

  private static final int EMPTY_SKETCH_SIZE_BYTES = 8;

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
   *          This must be a power of 2 and larger than 16.
   * @param seed using the seed is not recommended unless you really know why you need it.
   * @return resulting sketch of union.
   */
  public BytesWritable evaluate(final BytesWritable firstSketch, final BytesWritable secondSketch, 
      final int sketchSize, final long seed) {

    final Union union = SetOperation.builder().setSeed(seed).buildUnion(sketchSize);

    if ((firstSketch != null) && (firstSketch.getLength() >= EMPTY_SKETCH_SIZE_BYTES)) {
      union.update(new NativeMemory(firstSketch.getBytes()));
    }

    if ((secondSketch != null) && (secondSketch.getLength() >= EMPTY_SKETCH_SIZE_BYTES)) {
      union.update(new NativeMemory(secondSketch.getBytes()));
    }

    return new BytesWritable(union.getResult().toByteArray());
  }

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
   *          This must be a power of 2 and larger than 16.
   * @return resulting sketch of union.
   */
  public BytesWritable evaluate(final BytesWritable firstSketch, final BytesWritable secondSketch, 
      final int sketchSize) {
    return evaluate(firstSketch, secondSketch, sketchSize, DEFAULT_UPDATE_SEED);
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
  public BytesWritable evaluate(final BytesWritable firstSketch, final BytesWritable secondSketch) {
    return evaluate(firstSketch, secondSketch, DEFAULT_NOMINAL_ENTRIES, DEFAULT_UPDATE_SEED);
  }

}
