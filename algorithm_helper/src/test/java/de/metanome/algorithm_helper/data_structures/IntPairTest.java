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

import static org.junit.Assert.*;

import org.junit.Test;

import de.metanome.test_helper.CompareToTester;
import de.metanome.test_helper.EqualsAndHashCodeTester;

/**
 * Tests for {@link de.metanome.algorithm_helper.data_structures.IntPair}
 *
 * @author Jakob Zwiener
 */
public class IntPairTest {

  /**
   * Test method for {@link de.metanome.algorithm_helper.data_structures.IntPair#equals(Object)} and
   * {@link de.metanome.algorithm_helper.data_structures.IntPair#hashCode()}
   */
  @Test
  public void testEqualsHashCode() {
    // Setup
    // Expected values
    IntPair intPair = new IntPair(1, 2);
    IntPair equalIntPair = new IntPair(1, 2);
    IntPair notEqualIntPair1 = new IntPair(2, 2);
    IntPair notEqualIntPair2 = new IntPair(1, 3);

    // Execute functionality
    // Check result
    new EqualsAndHashCodeTester<IntPair>()
        .performBasicEqualsAndHashCodeChecks(intPair, equalIntPair, notEqualIntPair1,
          notEqualIntPair2);
  }

  /**
   * Test method for {@link IntPair#toString()}.
   */
  @Test
  public void testToString() {
    // Setup
    IntPair intPair = new IntPair(1, 2);
    IntPair otherIntPair = new IntPair(2, 2);
    // Expected values
    String expectedIntPairRepresentation = "IntPair{first=[1, 2]}";
    String expectedOtherIntPairRepresentation = "IntPair{first=[2, 2]}";

    // Execute functionality
    String actualIntPairRepresentation = intPair.toString();
    String actualOtherIntPairRepresentation = otherIntPair.toString();

    // Check result
    assertEquals(expectedIntPairRepresentation, actualIntPairRepresentation);
    assertEquals(expectedOtherIntPairRepresentation, actualOtherIntPairRepresentation);
  }

  /**
   * Test method for {@link de.metanome.algorithm_helper.data_structures.IntPair#compareTo(IntPair)}
   */
  @Test
  public void testCompareTo() {
    // Setup
    // Expected values
    CompareToTester<IntPair> compareToTester = new CompareToTester<>(new IntPair(4, 10));

    // Execute functionality
    // Check result
    compareToTester.performCompareToTestEqual(new IntPair(4, 10));
    compareToTester.performComparetoTestNotEqual(new IntPair(42, 23));
    compareToTester.performCompareToTestGreater(new IntPair(5, 9), new IntPair(4, 11));
    compareToTester.performCompareToTestNotGreater(new IntPair(3, 11));
    compareToTester.performCompareToTestSmaller(new IntPair(3, 10), new IntPair(4, 9));
    compareToTester.performCompareToTestNotSmaller(new IntPair(5, 2));
  }

}
