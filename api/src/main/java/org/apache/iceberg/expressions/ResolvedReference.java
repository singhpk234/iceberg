/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.expressions;

import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Types;

public class ResolvedReference<T> implements UnboundTerm<T>, Reference<T> {
  private final String name;
  private final int fieldId;

  public ResolvedReference(String name, int fieldId) {
    this.name = name;
    this.fieldId = fieldId;
  }

  @Override
  public String name() {
    return name;
  }

  public int fieldId() {
    return fieldId;
  }

  @Override
  public BoundTerm<T> bind(Types.StructType struct, boolean caseSensitive) {
    // assumption is that we always have the field id
    Schema schema = struct.asSchema();
    // Ignore caseSensitive because the field is referenced by id
    Types.NestedField field = schema.findField(fieldId);
    ValidationException.check(
        field != null,
        "Cannot find field with id '%s' in struct: %s, since we are resolving based on ID",
        fieldId,
        schema.asStruct());

    return new BoundReference<>(field, schema.accessorForField(field.fieldId()), name);
  }

  @Override
  public NamedReference<?> ref() {
    return new NamedReference<>(name);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ResolvedReference<?> that = (ResolvedReference<?>) o;
    return fieldId == that.fieldId && name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return 31 * fieldId + name.hashCode();
  }

  @Override
  public String toString() {
    return String.format("ref(name=\"%s\", fieldId=\"%s\")", name, fieldId);
  }
}
