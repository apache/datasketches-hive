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

package org.apache.datasketches.hive.kll;

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

@Description(name = "GetRank", value = "_FUNC_(sketch, [inclusive,] value)",
extended = " Returns a normalized rank of a given value from a given KllFloatsSketch."
+ " The optional boolean parameter inclusive (default: true) determines if the weight of the"
+ " given value is included in the rank or not."
+ " The returned rank is an approximation to the fraction of values of the distribution"
+ " that are less than the given value (mass of the distribution below the given value).")
@SuppressWarnings("deprecation")
public class GetRankUDF extends UDF {

  /**
   * Returns a normalized rank of a given value from a given sketch
   * @param serializedSketch serialized sketch
   * @param value the given value
   * @return rank
   */
  public Double evaluate(final BytesWritable serializedSketch, final float value) {
    return evaluate(serializedSketch, true, value);
  }

  /**
   * Returns a normalized rank of a given value from a given sketch
   * @param serializedSketch serialized sketch
   * @param inclusive if true the weight of the given item is included into the rank.
   * Otherwise the rank equals the sum of the weights of all items that are less than the given item
   * @param value the given value
   * @return rank
   */
  public Double evaluate(final BytesWritable serializedSketch, final Boolean inclusive, final float value) {
    if (serializedSketch == null) { return null; }
    final KllFloatsSketch sketch =
        KllFloatsSketch.heapify(BytesWritableHelper.wrapAsMemory(serializedSketch));
    if (sketch.isEmpty()) { return null; }
    return sketch.getRank(value, inclusive ? QuantileSearchCriteria.INCLUSIVE : QuantileSearchCriteria.EXCLUSIVE);
  }

}
