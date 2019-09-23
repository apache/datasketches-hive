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

package org.apache.datasketches.hive.tuple;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.datasketches.tuple.ArrayOfDoublesSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesSketchIterator;

class ArrayOfDoublesSketchStats {

  /**
   * Convert sketch to summary statistics.
   *
   * @param sketch ArrayOfDoublesSketch to convert to summary statistics.
   * @return An array of SummaryStatistics.
   */
  static SummaryStatistics[] sketchToSummaryStatistics(final ArrayOfDoublesSketch sketch) {
    final SummaryStatistics[] summaryStatistics = new SummaryStatistics[sketch.getNumValues()];
    for (int i = 0; i < sketch.getNumValues(); i++) {
      summaryStatistics[i] = new SummaryStatistics();
    }
    final ArrayOfDoublesSketchIterator it = sketch.iterator();
    while (it.next()) {
      final double[] values = it.getValues();
      for (int i = 0; i < it.getValues().length; i++) {
        summaryStatistics[i].addValue(values[i]);
      }
    }
    return summaryStatistics;
  }

}
