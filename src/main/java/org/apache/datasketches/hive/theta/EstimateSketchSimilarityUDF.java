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

package org.apache.datasketches.hive.theta;

import static org.apache.datasketches.thetacommon.ThetaUtil.DEFAULT_UPDATE_SEED;

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.theta.JaccardSimilarity;
import org.apache.datasketches.theta.Sketch;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

/**
 * Hive estimate sketch similarity UDF.
 *
 */
@SuppressWarnings("deprecation")
public class EstimateSketchSimilarityUDF extends UDF {

  /**
   * Main logic called by hive. Computes the jaccard similarity of two sketches of same or different column.
   *
   * @param firstSketchBytes
   *          first sketch to be compared.
   * @param secondSketchBytes
   *          second sketch to be compared.
   * @return the estimate of similarity of two sketches
   */
  public double evaluate(final BytesWritable firstSketchBytes, final BytesWritable secondSketchBytes) {
    Sketch firstSketch = null;
    if (firstSketchBytes != null && firstSketchBytes.getLength() > 0) {
      firstSketch = Sketch.wrap(BytesWritableHelper.wrapAsMemory(firstSketchBytes), DEFAULT_UPDATE_SEED);
    }

    Sketch secondSketch = null;
    if (secondSketchBytes != null && secondSketchBytes.getLength() > 0) {
      secondSketch = Sketch.wrap(BytesWritableHelper.wrapAsMemory(secondSketchBytes), DEFAULT_UPDATE_SEED);
    }

    final double[] jaccard = JaccardSimilarity.jaccard(firstSketch, secondSketch);
    return jaccard[1];
  }
}
