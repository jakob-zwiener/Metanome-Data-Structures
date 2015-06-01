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

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.longs.LongBigList;

import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link de.metanome.algorithm_helper.data_structures.PositionListIndex}
 */
public class PositionListIndexTest {

  protected PositionListIndexFixture fixture;

  @Before
  public void setUp() throws Exception {
    fixture = new PositionListIndexFixture();
  }

  /**
   * Test method for {@link de.metanome.algorithm_helper.data_structures.PositionListIndex#PositionListIndex()}
   * <p/> {@link de.metanome.algorithm_helper.data_structures.PositionListIndex} should be empty
   * after construction.
   */
  @Test
  public void testConstructor() {
    // Execute functionality
    PositionListIndex pli = new PositionListIndex();

    // Check result
    assertTrue(pli.clusters.isEmpty());
    assertTrue(pli.isEmpty());
  }

  /**
   * Test method for {@link de.metanome.algorithm_helper.data_structures.PositionListIndex#intersect(PositionListIndex)}
   *
   * Two {@link PositionListIndex} should be correctly intersected.
   */
  @Test
  public void testIntersect() {
    // Setup
    PositionListIndex firstPLI = fixture.getFirstPLI();
    PositionListIndex secondPLI = fixture.getSecondPLI();
    // Expected values
    PositionListIndex expectedPLI = fixture.getExpectedIntersectedPLI();

    // Execute functionality
    PositionListIndex actualIntersectedPLI = firstPLI.intersect(secondPLI);

    // Check result
    assertEquals(expectedPLI, actualIntersectedPLI);
  }

  /**
   * Test method for {@link PositionListIndex#intersect(PositionListIndex)}
   *
   * The intersection with a unique PLI should be unique.
   *
   * This is a regression test for the materialization and check of unique PLIs. There used to be an
   * array out of bounds access in buildMap.
   */
  @Test
  public void testIntersectUnique() {
    // Setup
    PositionListIndex uniquePLI = new PositionListIndex();
    PositionListIndex secondPLI = fixture.getSecondPLI();

    // Execute functionality
    PositionListIndex actualIntersectedPLI = uniquePLI.intersect(secondPLI);

    // Check result
    assertTrue(actualIntersectedPLI.isUnique());
  }

  /**
   * Test method for {{@link PositionListIndex#addOrExtendList(LongBigList, long, long)}}
   *
   * When adding a value beyond the size of the list, the list should be extended and padded with the SINGLETON_VALUE constant.
   */
  @Test
  public void testAddOrExtendList() {
    // Setup
    LongBigList list = new LongBigArrayBigList();
    PositionListIndex pli = fixture.getFirstPLI();
    long index1 = 23;
    long index2 = 11;
    // Expected values
    long expectedValue = 42;

    // Execute functionality
    pli.addOrExtendList(list, expectedValue, index1);
    pli.addOrExtendList(list, expectedValue, index2);

    // Check result
    assertEquals(expectedValue, (long) list.get(index1));
    for (long i = 0; i < index2; i++) {
      assertEquals(PositionListIndex.SINGLETON_VALUE, (long) list.get(i));
    }
    for (long i = index2 + 1; i < index1; i++) {
      assertEquals(PositionListIndex.SINGLETON_VALUE, (long) list.get(i));
    }
  }

  /**
   * Test method for {@link PositionListIndex#hashCode()}
   */
  @Test
  public void testHashCode() {
    // Setup
    PositionListIndex firstPLI = fixture.getFirstPLI();
    PositionListIndex permutatedfirstPLI = fixture.getPermutatedFirstPLI();

    // Execute functionality
    // Check result
    assertEquals(firstPLI, firstPLI);
    assertEquals(firstPLI.hashCode(), firstPLI.hashCode());
    assertEquals(firstPLI, permutatedfirstPLI);
    assertEquals(firstPLI.hashCode(), permutatedfirstPLI.hashCode());
  }

  /**
   * Test method for {@link de.metanome.algorithm_helper.data_structures.PositionListIndex#equals(Object)}
   */
  @Test
  public void testEquals() {
    // Setup
    PositionListIndex firstPLI = fixture.getFirstPLI();
    PositionListIndex permutatedfirstPLI = fixture.getPermutatedFirstPLI();
    PositionListIndex secondPLI = fixture.getSecondPLI();
    PositionListIndex supersetOfFirstPLI = fixture.getSupersetOfFirstPLI();

    // Execute functionality
    // Check result
    assertEquals(firstPLI, firstPLI);
    assertEquals(firstPLI, permutatedfirstPLI);
    assertNotEquals(firstPLI, secondPLI);
    assertNotEquals(firstPLI, supersetOfFirstPLI);
  }

  /**
   * Test method for {@link PositionListIndex#asHashMap()}
   *
   * A {@link PositionListIndex} should return a valid and correct HashMap.
   */
  @Test
  public void testAsHashMap() {
    // Setup
    PositionListIndex firstPLI = fixture.getFirstPLI();

    //expected Values
    Long2LongOpenHashMap expectedHashMap = fixture.getFirstPLIAsHashMap();

    assertEquals(expectedHashMap, firstPLI.asHashMap());
  }

  /**
   * Test method for {@link PositionListIndex#asList()}
   */
  @Test
  public void testAsList() {
    // Setup
    PositionListIndex firstPLI = fixture.getFirstPLI();

    //expected Values
    LongBigList expectedList = fixture.getFirstPLIAsList();

    assertEquals(expectedList, firstPLI.asList());
  }

  /**
   * Test method for {@link PositionListIndex#size()} <p/> Size should return the correct number of
   * noon unary clusters of the {@link PositionListIndex}.
   */
  @Test
  public void testSize() {
    // Setup
    PositionListIndex pli = fixture.getFirstPLI();

    // Execute functionality
    // Check result
    assertEquals(fixture.getFirstPLISize(), pli.size());
  }

  /**
   * Test method for {@link PositionListIndex#isEmpty()}, {@link PositionListIndex#isUnique()} <p/>
   * Empty plis should return true on isEmpty and is Unique.
   */
  @Test
  public void testIsEmptyUnique() {
    // Setup
    List<LongArrayList> clusters = new LinkedList<>();
    PositionListIndex emptyPli = new PositionListIndex(clusters);
    PositionListIndex nonEmptyPli = fixture.getFirstPLI();

    // Execute functionality
    // Check result
    assertTrue(emptyPli.isEmpty());
    assertTrue(emptyPli.isUnique());
    assertFalse(nonEmptyPli.isEmpty());
    assertFalse(nonEmptyPli.isUnique());
  }

  /**
   * Test method for {@link PositionListIndex#getRawKeyError()} <p/> The key error should be
   * calculated and updated correctly.
   */
  @Test
  public void testGetRawKeyError() {
    // Setup
    PositionListIndex firstPli = fixture.getFirstPLI();
    PositionListIndex secondPli = fixture.getSecondPLI();

    // Execute functionality
    // Check result
    assertEquals(fixture.getExpectedFirstPLIRawKeyError(), firstPli.getRawKeyError());
    assertEquals(fixture.getExpectedSecondPLIRawKeyError(), secondPli.getRawKeyError());
    assertEquals(fixture.getExpectedIntersectedPLIRawKeyError(),
                 firstPli.intersect(secondPli).getRawKeyError());
  }

  /**
   * Test method for {@link PositionListIndex#clone()}. Clone() should create a deep copy of the
   * called {@link de.metanome.algorithm_helper.data_structures.PositionListIndex}.
   */
  @Test
  public void testClone() {
    // Setup
    PositionListIndex pli = fixture.getFirstPLI();

    // Execute functionality
    PositionListIndex copy = pli.clone();

    // Check result
    assertEquals(pli, copy);
    assertNotSame(pli, copy);
    for (int i = 0; i < pli.getClusters().size(); i++) {
      assertNotSame(pli.getClusters().get(i), copy.getClusters().get(i));
      assertEquals(pli.getClusters().get(i), copy.getClusters().get(i));
    }
  }
}
