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

package org.apache.datasketches.hive.frequencies;

import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

@Description(name = "Union", value = "_FUNC_(sketch) - "
    + "Returns an ItemsSketch<String> in a serialized form as a binary blob."
    + " Input values must also be serialized sketches.")
public class UnionStringsSketchUDAF extends UnionItemsSketchUDAF<String> {

  @Override
  GenericUDAFEvaluator createEvaluator() {
    return new UnionStringsSketchEvaluator();
  }

  static class UnionStringsSketchEvaluator extends UnionItemsSketchEvaluator<String> {

    UnionStringsSketchEvaluator() {
      super(new ArrayOfStringsSerDe());
    }

  }

}
