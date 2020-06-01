/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.hive.cpc;

import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;

import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.cpc.CpcUnion;
import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

/**
 * Hive union sketch UDF.
 */
public class UnionSketchUDF extends UDF {

  /**
   * Union two sketches given explicit lgK and seed
   *
   * @param firstSketch
   *   first sketch to be unioned.
   * @param secondSketch
   *   second sketch to be unioned.
   * @param lgK
   *   final output lgK
   *   This must be between 4 and 26.
   * @param seed
   *   update seed
   * @return resulting sketch of union.
   */
  public BytesWritable evaluate(final BytesWritable firstSketch, final BytesWritable secondSketch,
      final int lgK, final long seed) {

    final CpcUnion union = new CpcUnion(lgK, seed);

    if (firstSketch != null) {
      union.update(CpcSketch.heapify(BytesWritableHelper.wrapAsMemory(firstSketch), seed));
    }

    if (secondSketch != null) {
      union.update(CpcSketch.heapify(BytesWritableHelper.wrapAsMemory(secondSketch), seed));
    }

    return new BytesWritable(union.getResult().toByteArray());
  }

  /**
   * Union two sketches given explicit lgK and using default seed
   *
   * @param firstSketch
   *   first sketch to be unioned.
   * @param secondSketch
   *   second sketch to be unioned.
   * @param lgK
   *   final output lgK
   *   This must be between 4 and 26.
   * @return resulting sketch of union.
   */
  public BytesWritable evaluate(final BytesWritable firstSketch, final BytesWritable secondSketch,
      final int lgK) {
    return evaluate(firstSketch, secondSketch, lgK, DEFAULT_UPDATE_SEED);
  }

  /**
   * Union two sketches using default lgK an seed
   *
   * @param firstSketch
   *    first sketch to be unioned.
   * @param secondSketch
   *    second sketch to be unioned.
   * @return resulting sketch of union.
   */
  public BytesWritable evaluate(final BytesWritable firstSketch, final BytesWritable secondSketch) {
    return evaluate(firstSketch, secondSketch, SketchEvaluator.DEFAULT_LG_K, DEFAULT_UPDATE_SEED);
  }

}
