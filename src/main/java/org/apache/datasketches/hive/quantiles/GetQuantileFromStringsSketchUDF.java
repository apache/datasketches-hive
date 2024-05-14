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

import java.util.Comparator;

import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

@Description(name = "GetQuantile", value = "_FUNC_(sketch, [inclusive,] fraction)",
    extended = " Returns a quantile value from a given ItemsSketch<String> sketch."
    + " A single value for a given fraction is returned."
    + " The optional boolean parameter 'inclusive' (default: true) determines if the result includes"
    + " values less than or equal to the fraction or, if false, only values strictly less than"
    + " the fraction."
    + " The fraction represents a normalized rank, and must be from 0 to 1 inclusive."
    + " For example, a fraction of 0.5 corresponds to 50th percentile, which is"
    + " the median value of the distribution (the number separating the higher half"
    + " of the probability distribution from the lower half).")
@SuppressWarnings("deprecation")
public class GetQuantileFromStringsSketchUDF extends UDF {

  /**
   * Returns a quantile value from a given sketch. Equivalent to calling
   * GetQuantile(sketch, true, fraction)
   * @param serializedSketch serialized sketch
   * @param fraction value from 0 to 1 inclusive
   * @return quantile value
   */
  public String evaluate(final BytesWritable serializedSketch, final double fraction) {
    return evaluate(serializedSketch, true, fraction);
  }

  /**
   * Returns a quantile value from a given sketch
   * @param serializedSketch serialized sketch
   * @param inclusive if true, the given rank is considered inclusive (includes weight of an item)
   * @param fraction value from 0 to 1 inclusive
   * @return quantile value
   */
  public String evaluate(final BytesWritable serializedSketch, final Boolean inclusive, final double fraction) {
    if (serializedSketch == null) { return null; }
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(
      String.class,
      BytesWritableHelper.wrapAsMemory(serializedSketch),
      Comparator.naturalOrder(),
      new ArrayOfStringsSerDe()
    );
    if (sketch.isEmpty()) { return null;}
    return sketch.getQuantile(fraction,
      inclusive ? QuantileSearchCriteria.INCLUSIVE : QuantileSearchCriteria.EXCLUSIVE);
  }

}
