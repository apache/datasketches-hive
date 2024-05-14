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

package org.apache.datasketches.hive.quantiles;

import java.util.List;

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

@Description(
  name = "GetQuantiles",
  value = "_FUNC_(sketch, [inclusive,] fractions...) or _FUNC_(sketch, [inclusive,] number)",
  extended = "Returns quantile values from a given DoublesSketch based on a given"
  + " list of fractions or a number of evenly spaced fractions."
  + " The optional boolean parameter 'inclusive' determines if the interval is inclusive,"
  + " which is inclusive of the left split point and exclusive of the right split point, or"
  + " the alternative of exclusive of the split point and inclusive of the right split point."
  + " Defaults to inclusive (of left split point) when not specified."
  + " The fractions represent normalized ranks, and must be from 0 to 1 inclusive."
  + " For example, a fraction of 0.5 corresponds to 50th percentile,"
  + " which is the median value of the distribution (the number separating the higher"
  + " half of the probability distribution from the lower half)."
  + " The number of evenly spaced fractions must be a positive integer greater than 0."
  + " A value of 1 will return the min value (normalized rank of 0.0)."
  + " A value of 2 will return the min and the max value (ranks 0.0 amd 1.0)."
  + " A value of 3 will return the min, the median and the max value (ranks 0.0, 0.5, and 1.0), etc.")
@SuppressWarnings("deprecation")
public class GetQuantilesFromDoublesSketchUDF extends UDF {

  /**
   * Returns a list of quantile values from a given sketch. Equivalent to calling
   * GetQuantiles(sketch, true, fractions...)
   * @param serializedSketch serialized sketch
   * @param fractions list of values from 0 to 1 inclusive
   * @return list of quantile values
   */
  public List<Double> evaluate(final BytesWritable serializedSketch, final Double... fractions) {
    return evaluate(serializedSketch, true, fractions);
  }

  /**
   * Returns a list of quantile values from a given sketch
   * @param serializedSketch serialized sketch
   * @param inclusive if true, the interval is inclusive of the left split point and exclusive of the right split point
   * @param fractions list of values from 0 to 1 inclusive
   * @return list of quantile values
   */
  public List<Double> evaluate(final BytesWritable serializedSketch, final Boolean inclusive, final Double... fractions) {
    if (serializedSketch == null) { return null; }
    final DoublesSketch sketch = DoublesSketch.wrap(BytesWritableHelper.wrapAsMemory(serializedSketch));
    if (sketch.isEmpty()) { return null; }
    return Util.primitivesToList(sketch.getQuantiles(Util.objectsToPrimitives(fractions),
        (inclusive ? QuantileSearchCriteria.INCLUSIVE : QuantileSearchCriteria.EXCLUSIVE)));
  }

  /**
   * Returns a list of quantile values from a given sketch. Equivalent to calling
   * GetQuantiles(sketch, true, number)
   * @param serializedSketch serialized sketch
   * @param number of evenly spaced fractions
   * @return list of quantile values
   */
  public List<Double> evaluate(final BytesWritable serializedSketch, final int number) {
    return evaluate(serializedSketch, true, number);
  }

  /**
   * Returns a list of quantile values from a given sketch
   * @param serializedSketch serialized sketch
   * @param inclusive if true, the interval is inclusive of the left split point and exclusive of the right split point
   * @param number of evenly spaced fractions
   * @return list of quantile values
   */
  public List<Double> evaluate(final BytesWritable serializedSketch, final Boolean inclusive, final int number) {
    if (serializedSketch == null) { return null; }
    final DoublesSketch sketch = DoublesSketch.wrap(BytesWritableHelper.wrapAsMemory(serializedSketch));
    if (sketch.isEmpty()) { return null; }

    double[] quantiles = null;
    if (number == 1) {
      quantiles = new double[1];
      quantiles[0] = sketch.getMinItem();
    } else if (number == 2) {
      quantiles = new double[2];
      quantiles[0] = sketch.getMinItem();
      quantiles[1] = sketch.getMaxItem();
    } else if (number > 2) {
      final double[] ranks = new double[number];
      final double delta = 1.0 / (number - 1);
      for (int i = 0; i < number; i++) {
        ranks[i] = i * delta;
      }
      quantiles = sketch.getQuantiles(ranks,
          (inclusive ? QuantileSearchCriteria.INCLUSIVE : QuantileSearchCriteria.EXCLUSIVE));
      quantiles[number - 1] = sketch.getMaxItem(); // to ensure the max value is exact
    }

    if (quantiles == null) { return null; }
    return Util.primitivesToList(quantiles);
  }

}
