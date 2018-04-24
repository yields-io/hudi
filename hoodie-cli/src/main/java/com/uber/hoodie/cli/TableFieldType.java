/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.cli;

import java.util.Comparator;

/**
 * Type of fields in the table
 */
public enum TableFieldType {
  // Use double types for comparison
  NUMERIC(new Comparator<Object>() {

    @Override
    public int compare(Object o1, Object o2) {
      Double val1 = Double.valueOf(o1.toString());
      Double val2 = Double.valueOf(o2.toString());
      return val1.compareTo(val2);
    }
  }),
  // Use string comparison
  TEXT(new Comparator<Object>() {

    @Override
    public int compare(Object o1, Object o2) {
      String val1 = o1.toString();
      String val2 = o2.toString();
      return val1.compareTo(val2);
    }
  });

  private final Comparator<Object> comparator;

  TableFieldType(Comparator<Object> comparator) {
    this.comparator = comparator;
  }

  public Comparator<Object> getComparator() {
    return comparator;
  }
}
