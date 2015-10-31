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

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.*;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.Before;
import org.junit.Test;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

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
   * <p>
   * Two {@link PositionListIndex} should be correctly intersected.
   */
  @Test
  public void testIntersect() throws PLIBuildingException {
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
   * <p>
   * The intersection with a unique PLI should be unique.
   * <p>
   * This is a regression test for the materialization and check of unique PLIs. There used to be an
   * array out of bounds access in buildMap.
   */
  @Test
  public void testIntersectUnique() throws PLIBuildingException {
    // Setup
    PositionListIndex uniquePLI = new PositionListIndex();
    PositionListIndex secondPLI = fixture.getSecondPLI();

    // Execute functionality
    PositionListIndex actualIntersectedPLI = uniquePLI.intersect(secondPLI);

    // Check result
    assertTrue(actualIntersectedPLI.isUnique());
  }

  /**
   * Test method for {{@link PositionListIndex#addOrExtendList(IntList, int, int)}}
   * <p>
   * When adding a value beyond the size of the list, the list should be extended and padded with the SINGLETON_VALUE constant.
   */
  @Test
  public void testAddOrExtendList() {
    // Setup
    IntList list = new IntArrayList();
    PositionListIndex pli = fixture.getFirstPLI();
    int index1 = 23;
    int index2 = 11;
    // Expected values
    int expectedValue = 42;

    // Execute functionality
    pli.addOrExtendList(list, expectedValue, index1);
    pli.addOrExtendList(list, expectedValue, index2);

    // Check result
    assertEquals(expectedValue, (int) list.get(index1));
    for (int i = 0; i < index2; i++) {
      assertEquals(PositionListIndex.SINGLETON_VALUE, (int) list.get(i));
    }
    for (int i = index2 + 1; i < index1; i++) {
      assertEquals(PositionListIndex.SINGLETON_VALUE, (int) list.get(i));
    }
  }

  /**
   * Test method for {@link PositionListIndex#hashCode()}, and {@link PositionListIndex#equals(Object)}
   */
  @Test
  public void testEqualsHashCode() {
    // Setup
    PositionListIndex firstPLI = fixture.getFirstPLI();
    PositionListIndex permutatedfirstPLI = fixture.getPermutatedFirstPLI();
    PositionListIndex secondPLI = fixture.getSecondPLI();
    PositionListIndex supersetOfFirstPLI = fixture.getSupersetOfFirstPLI();
    PositionListIndex firstPLIDifferentNumberOfRows = fixture.getFirstPLI();
    firstPLIDifferentNumberOfRows.numberOfRows = 42;


    // Execute functionality
    // Check result
    assertEquals(firstPLI, firstPLI);
    assertEquals(firstPLI.hashCode(), firstPLI.hashCode());
    assertEquals(firstPLI, permutatedfirstPLI);
    assertEquals(firstPLI.hashCode(), permutatedfirstPLI.hashCode());
    assertNotEquals(firstPLI, secondPLI);
    assertNotEquals(firstPLI, supersetOfFirstPLI);
    assertNotEquals(firstPLI, firstPLIDifferentNumberOfRows);
    assertNotEquals(firstPLI.hashCode(), firstPLIDifferentNumberOfRows.hashCode());
  }

  /**
   * Test method for {@link PositionListIndex#asHashMap()}
   * <p>
   * A {@link PositionListIndex} should return a valid and correct HashMap.
   */
  @Test
  public void testAsHashMap() {
    // Setup
    PositionListIndex firstPLI = fixture.getFirstPLI();

    //expected Values
    Int2IntOpenHashMap expectedHashMap = fixture.getFirstPLIAsHashMap();

    assertEquals(expectedHashMap, firstPLI.asHashMap());
  }

  /**
   * Test method for {@link PositionListIndex#asArray()}
   */
  @Test
  public void testAsArray() throws PLIBuildingException {
    // Setup
    PositionListIndex firstPLI = fixture.getFirstPLI();

    //expected Values
    int[] expectedList = fixture.getFirstPLIAsArray();

    assertArrayEquals(expectedList, firstPLI.asArray());
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
    List<IntArrayList> clusters = new LinkedList<>();
    PositionListIndex emptyPli = new PositionListIndex(clusters, 0);
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
  public void testGetRawKeyError() throws PLIBuildingException {
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
   * Test method for {@link PositionListIndex#getNumberOfRows()}
   */
  @Test
  public void testNumberOfRows() {
    // Setup
    // Expected values
    int expectedNumberOfRows = 42;
    PositionListIndex positionListIndex = new PositionListIndex(new LinkedList<IntArrayList>(), expectedNumberOfRows);

    // Execute functionality
    // Check result
    assertEquals(expectedNumberOfRows, positionListIndex.getNumberOfRows());
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

  /**
   * Test method for {@link PositionListIndex#toString}
   */
  @Test
  public void testToString() {
    // Setup
    PositionListIndex firstPli = fixture.getFirstPLI();
    PositionListIndex secondPli = fixture.getSecondPLI();

    // Execute functionality
    String actualFirstPliRepresentation = firstPli.toString();
    String actualSecondPliRepresentation = secondPli.toString();

    // Check result
    assertEquals(fixture.getExpectedFirstPliToString(), actualFirstPliRepresentation);
    assertEquals(fixture.getExpectedSecondPliToString(), actualSecondPliRepresentation);
  }

  /**
   * Tests whether the plis can be serialized and deserialized correctly.
   */
  @Test
  public void testSerialization() throws IOException, ClassNotFoundException {
    // Setup
    // Expected values
    PositionListIndex expectedFirstPli = fixture.getFirstPLI();
    PositionListIndex expectedSecondPli = fixture.getSecondPLI();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(out);

    // Execute functionality
    oos.writeObject(expectedFirstPli);
    oos.writeObject(expectedSecondPli);
    byte[] written = out.toByteArray();
    ByteArrayInputStream in = new ByteArrayInputStream(written);
    ObjectInputStream ois = new ObjectInputStream(in);
    PositionListIndex actualFirstPli = (PositionListIndex) ois.readObject();
    PositionListIndex actualSecondPli = (PositionListIndex) ois.readObject();

    // Check result
    assertEquals(expectedFirstPli, actualFirstPli);
    assertEquals(expectedSecondPli, actualSecondPli);
  }

}
