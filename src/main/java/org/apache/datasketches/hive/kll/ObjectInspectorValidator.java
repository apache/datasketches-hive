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

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

final class ObjectInspectorValidator {

  static void validateCategoryPrimitive(
      final ObjectInspector inspector, final int index) throws SemanticException {
    if (inspector.getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(index, "Primitive argument expected, but "
          + inspector.getCategory().name() + " was recieved");
    }
  }

  static void validateGivenPrimitiveCategory(final ObjectInspector inspector, final int index,
      final PrimitiveObjectInspector.PrimitiveCategory category) throws SemanticException
  {
    validateCategoryPrimitive(inspector, index);
    final PrimitiveObjectInspector primitiveInspector = (PrimitiveObjectInspector) inspector;
    if (primitiveInspector.getPrimitiveCategory() != category) {
      throw new UDFArgumentTypeException(index, category.name() + " value expected as the argument "
          + (index + 1) + " but " + primitiveInspector.getPrimitiveCategory().name() + " was received");
    }
  }

}
