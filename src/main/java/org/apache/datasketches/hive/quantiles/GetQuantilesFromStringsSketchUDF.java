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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

@Description(
    name = "GetQuantiles",
    value = "_FUNC_(sketch, [inclusive,] fractions...) or _FUNC_(sketch, [inclusive,] number)",
    extended = "Returns quantile values from a given ItemsSketch<String> based on a given"
    + " list of fractions or a number of evenly spaced fractions."
    + " The optional boolean parameter 'inclusive' (default: true) determines if the result includes"
    + " values less than or equal to each target fraction or, if false, only values strictly less than"
    + " each target fraction."
    + " The fractions represent normalized ranks, and must be from 0 to 1 inclusive."
    + " For example, a fraction of 0.5 corresponds to 50th percentile,"
    + " which is the median value of the distribution (the number separating the higher"
    + " half of the probability distribution from the lower half)."
    + " The number of evenly spaced fractions must be a positive integer greater than 0."
    + " A value of 1 will return the min value (normalized rank of 0.0)."
    + " A value of 2 will return the min and the max value (ranks 0.0 amd 1.0)."
    + " A value of 3 will return the min, the median and the max value (ranks 0.0, 0.5, and 1.0), etc.")
@SuppressWarnings("deprecation")
public class GetQuantilesFromStringsSketchUDF extends UDF {

  /**
   * Returns a list of quantile values from a given sketch. Equivalent to calling
   * GetQuantiles(sketch, true, fractions...)
   * @param serializedSketch serialized sketch
   * @param fractions list of values from 0 to 1 inclusive
   * @return list of quantile values
   */
  public List<String> evaluate(final BytesWritable serializedSketch, final Double... fractions) {
    return evaluate(serializedSketch, true, fractions);
  }

  /**
   * Returns a list of quantile values from a given sketch
   * @param serializedSketch serialized sketch
   * @param inclusive if true, the given ranks are considered inclusive (include weight of an item)
   * @param fractions list of values from 0 to 1 inclusive
   * @return list of quantile values
   */
  public List<String> evaluate(final BytesWritable serializedSketch, final Boolean inclusive, final Double... fractions) {
    if (serializedSketch == null) { return null; }
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(
      String.class,
      BytesWritableHelper.wrapAsMemory(serializedSketch),
      Comparator.naturalOrder(),
      new ArrayOfStringsSerDe()
    );
    return Arrays.asList(sketch.getQuantiles(Util.objectsToPrimitives(fractions),
    (inclusive ? QuantileSearchCriteria.INCLUSIVE : QuantileSearchCriteria.EXCLUSIVE)));
  }

  /**
   * Returns a list of quantile values from a given sketch Equivalent to calling
   * GetQuantiles(sketch, true, number)
   * @param serializedSketch serialized sketch
   * @param number of evenly spaced fractions
   * @return list of quantile values
   */
  public List<String> evaluate(final BytesWritable serializedSketch, final int number) {
    return evaluate(serializedSketch, true, number);
  }

  /**
   * Returns a list of quantile values from a given sketch
   * @param serializedSketch serialized sketch
   * @param inclusive if true, the given ranks are considered inclusive (include weight of an item)
   * @param number of evenly spaced fractions
   * @return list of quantile values
   */
  public List<String> evaluate(final BytesWritable serializedSketch, final Boolean inclusive, final int number) {
    if (serializedSketch == null) { return null; }
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(
      String.class,
      BytesWritableHelper.wrapAsMemory(serializedSketch),
      Comparator.naturalOrder(),
      new ArrayOfStringsSerDe()
    );
    if (sketch.isEmpty()) { return null; }

    String[] quantiles = null;
    if (number == 1) {
      quantiles = new String[1];
      quantiles[0] = sketch.getMinItem();
    } else if (number == 2) {
      quantiles = new String[2];
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
    return Arrays.asList(quantiles);
  }

}
