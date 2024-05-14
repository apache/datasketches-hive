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

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

@Description(name = "GetQuantile", value = "_FUNC_(sketch, [inclusive,] fraction)",
    extended = " Returns a quantile value from a given DoublesSketch sketch."
    + " A single value for a given fraction is returned."
    + " The optional boolean parameter 'inclusive' determines if the interval is inclusive,"
    + " which is inclusive of the left split point and exclusive of the right split point, or"
    + " the alternative of exclusive of the split point and inclusive of the right split point."
    + " Defaults to inclusive (of left split point) when not specified."
    + " The fraction represents a normalized rank, and must be from 0 to 1 inclusive."
    + " For example, a fraction of 0.5 corresponds to 50th percentile, which is"
    + " the median value of the distribution (the number separating the higher half"
    + " of the probability distribution from the lower half).")
@SuppressWarnings("deprecation")
public class GetQuantileFromDoublesSketchUDF extends UDF {

  /**
   * Returns a quantile value from a given sketch. Equivalent to calling
   * GetQuantile(sketch, true, fraction)
   * @param serializedSketch serialized sketch
   * @param fraction value from 0 to 1 inclusive
   * @return quantile value
   */
  public Double evaluate(final BytesWritable serializedSketch, final double fraction) {
    return evaluate(serializedSketch, true, fraction);
  }

  /**
   * Returns a quantile value from a given sketch
   * @param serializedSketch serialized sketch
   * @param inclusive if true, the interval is inclusive of the left split point and exclusive of the right split point
   * @param fraction value from 0 to 1 inclusive
   * @return quantile value
   */
  public Double evaluate(final BytesWritable serializedSketch, final Boolean inclusive, final double fraction) {
    if (serializedSketch == null) { return null; }
    final DoublesSketch sketch = DoublesSketch.wrap(BytesWritableHelper.wrapAsMemory(serializedSketch));
    if (sketch.isEmpty()) { return null; }
    return sketch.getQuantile(fraction,
      inclusive ? QuantileSearchCriteria.INCLUSIVE : QuantileSearchCriteria.EXCLUSIVE);
  }

}
