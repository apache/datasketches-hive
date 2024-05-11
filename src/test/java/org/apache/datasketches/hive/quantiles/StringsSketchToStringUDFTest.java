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

import org.apache.hadoop.io.BytesWritable;

import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.quantiles.ItemsSketch;

import org.testng.annotations.Test;
import org.testng.Assert;

@SuppressWarnings("javadoc")
public class StringsSketchToStringUDFTest {

  static final Comparator<String> COMPARATOR = Comparator.naturalOrder();
  static final ArrayOfItemsSerDe<String> SERDE = new ArrayOfStringsSerDe();

  @Test
  public void nullSketch() {
    final String result = new StringsSketchToStringUDF().evaluate(null);
    Assert.assertNull(result);
  }

  @Test
  public void normalCase() {
    final ItemsSketch<String> sketch = ItemsSketch.getInstance(COMPARATOR);
    final String result = new StringsSketchToStringUDF().evaluate(new BytesWritable(sketch.toByteArray(SERDE)));
    Assert.assertNotNull(result);
    Assert.assertTrue(result.length() > 0);
  }

}
