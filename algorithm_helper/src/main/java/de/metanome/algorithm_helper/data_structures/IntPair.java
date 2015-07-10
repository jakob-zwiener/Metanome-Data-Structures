/*
 * Copyright 2015 by the Metanome project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.metanome.algorithm_helper.data_structures;

import java.util.Arrays;

/**
 * A pair of int values.
 * @author Jakob Zwiener
 */
public class IntPair implements Comparable<IntPair> {

  private int[] first;

  public IntPair(int... first) {
    this.first = first;
  }

  @Override public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final IntPair intPair = (IntPair) o;

    return Arrays.equals(first, intPair.first);

  }

  @Override public int hashCode() {
    return first != null ? Arrays.hashCode(first) : 0;
  }

  @Override public String toString() {
    return "IntPair{" +
      "first=" + Arrays.toString(first) +
      '}';
  }

  @Override
  public int compareTo(IntPair other) {
    if (other == null) {
      return 1;
    }

    int sizeDifference = this.first.length - other.first.length;

    if (sizeDifference != 0) {
      return sizeDifference;
    }

    if (sizeDifference == 0) {
      for (int i = 0; i < this.first.length; i++) {
        int difference = this.first[i] - other.first[i];
        if (difference != 0) {
          return difference;
        }
      }
    }

    return 0;

  }
}
