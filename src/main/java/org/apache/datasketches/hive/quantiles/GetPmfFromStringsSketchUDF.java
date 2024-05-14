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
import java.util.List;

import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

@Description(
    name = "GetPMF",
    value = "_FUNC_(sketch, [inclusive,] split points...)",
    extended = "Returns an approximation to the Probability Mass Function (PMF)"
    + " from a sketch given a set of split points (values)."
    + " The optional boolean parameter 'inclusive' determines if the interval is inclusive,"
    + " which is inclusive of the left split point and exclusive of the right split point, or"
    + " the alternative of exclusive of the split point and inclusive of the right split point."
    + " Defaults to inclusive (of left split point) when not specified."
    + " Split points are an array of M unique, monotonically increasing values"
    + " that divide the domain into M+1 consecutive disjoint intervals."
    + " The function returns an array of M+1 doubles, each of which is an approximation"
    + " to the fraction of the values that fell into one of those intervals."
    + " The definition of an interval is inclusive of the left split point and exclusive"
    + " of the right split point")
@SuppressWarnings("deprecation")
public class GetPmfFromStringsSketchUDF extends UDF {

  /**
   * Returns a list of fractions (PMF) from a given sketch. Equivalent to calling
   * GetPMF(sketch, true, splitPoints...)
   * @param serializedSketch serialized sketch
   * @param splitPoints list of unique and monotonically increasing values
   * @return list of fractions from 0 to 1
   */
  public List<Double> evaluate(final BytesWritable serializedSketch, final String... splitPoints) {
    return evaluate(serializedSketch, true, splitPoints);
  }

   /**
   * Returns a list of fractions (PMF) from a given sketch
   * @param serializedSketch serialized sketch
   * @param inclusive if true, the interval is inclusive of the left split point and exclusive of the right split point
   * @param splitPoints list of unique and monotonically increasing values
   * @return list of fractions from 0 to 1
   */
  public List<Double> evaluate(final BytesWritable serializedSketch, Boolean inclusive, final String... splitPoints) {
    if (serializedSketch == null) { return null; }
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(
      String.class,
      BytesWritableHelper.wrapAsMemory(serializedSketch),
      Comparator.naturalOrder(),
      new ArrayOfStringsSerDe()
    );
    if (sketch.isEmpty()) { return null; }
    final double[] pmf = sketch.getPMF(splitPoints,
                         (inclusive ? QuantileSearchCriteria.INCLUSIVE : QuantileSearchCriteria.EXCLUSIVE));
    if (pmf == null) { return null; }
    return Util.primitivesToList(pmf);
  }



}
