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

/**
 * A pair of int values.
 * @author Jakob Zwiener
 */
public class IntPair implements Comparable<IntPair> {

  private int first;
  private int second;

  public IntPair(int first, int second) {
    this.first = first;
    this.second = second;
  }

  @Override
  public int hashCode() {
    int result = first;
    result = 31 * result + second;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    IntPair other = (IntPair) obj;
    if (first != other.first) {
      return false;
    }
    return second == other.second;
  }

  public int getFirst() {
    return this.first;
  }

  public void setFirst(int first) {
    this.first = first;
  }

  public int getSecond() {
    return this.second;
  }

  public void setSecond(int second) {
    this.second = second;
  }

  @Override
  public String toString() {
    return "IntPair{" + first + ", " + second + '}';
  }

  @Override
  public int compareTo(IntPair other) {
    if (other == null) {
      return 1;
    }

    if (other.first == this.first) {
      return this.second - other.second;
    }
    return this.first - other.first;
  }

}
