/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.hll;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.TgtHllType;
import com.yahoo.sketches.hll.Union;

/**
 * Hive union sketch UDF.
 */
public class UnionSketchUDF extends UDF {

  /**
   * Union two sketches given explicit lgK and target HLL type
   *
   * @param firstSketch
   *   first sketch to be unioned.
   * @param secondSketch
   *          second sketch to be unioned.
   * @param lgK
   *   final output lgK
   *   This must be between 4 and 21.
   * @param type
   *   final output HLL type
   * @return resulting sketch of union.
   */
  public BytesWritable evaluate(final BytesWritable firstSketch, final BytesWritable secondSketch,
      final int lgK, final String type) {

    final TgtHllType hllType = TgtHllType.valueOf(type);
    final Union union = new Union(lgK);

    if (firstSketch != null) {
      union.update(HllSketch.heapify(Memory.wrap(firstSketch.getBytes())));
    }

    if (secondSketch != null) {
      union.update(HllSketch.heapify(Memory.wrap(secondSketch.getBytes())));
    }

    return new BytesWritable(union.getResult(hllType).toCompactByteArray());
  }

  /**
   * Union two sketches given explicit lgK and using default target HLL type
   *
   * @param firstSketch
   *   first sketch to be unioned.
   * @param secondSketch
   *   second sketch to be unioned.
   * @param lgK
   *   final output lgK
   *   This must be between 4 and 21.
   * @return resulting sketch of union.
   */
  public BytesWritable evaluate(final BytesWritable firstSketch, final BytesWritable secondSketch,
      final int lgK) {
    return evaluate(firstSketch, secondSketch, lgK, SketchEvaluator.DEFAULT_HLL_TYPE.toString());
  }

  /**
   * Union two sketches using default lgK an target HLL type
   *
   * @param firstSketch
   *          first sketch to be unioned.
   * @param secondSketch
   *          second sketch to be unioned.
   * @return resulting sketch of union.
   */
  public BytesWritable evaluate(final BytesWritable firstSketch, final BytesWritable secondSketch) {
    return evaluate(firstSketch, secondSketch, SketchEvaluator.DEFAULT_LG_K,
        SketchEvaluator.DEFAULT_HLL_TYPE.toString());
  }

}
