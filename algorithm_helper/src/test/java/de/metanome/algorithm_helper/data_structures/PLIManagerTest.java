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
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import it.unimi.dsi.fastutil.ints.IntArrayList;

/**
 * Tests for {@link PLIManager}
 * @author Jakob Zwiener
 */
public class PLIManagerTest {

  protected PLIManagerFixture fixture;
  protected PLIManager pliManager;

  @Before
  public void setUp() throws ColumnIndexOutOfBoundsException {
    fixture = new PLIManagerFixture();
    pliManager = new PLIManager(fixture.getPlis(), fixture.numberOfColumns(), Integer.MAX_VALUE);
  }

  /**
   * Test method for {@link PLIManager#PLIManager(PositionListIndex[], int, int)}
   *
   * The columnc combination representing all columns should be correctly constructed.
   */
  @Test
  public void testConstructorAllColumnCombination() throws ColumnIndexOutOfBoundsException {
    // Setup
    final int numberOfColumns = 42;
    // Expected values
    ColumnCombinationBitset expectedAllColumnsCombination = new ColumnCombinationBitset();
    for (int columnIndex = 0; columnIndex < 42; columnIndex++) {
      expectedAllColumnsCombination.addColumn(columnIndex);
    }

    // Execute functionality
    PLIManager
        actualPliManager =
        new PLIManager(new PositionListIndex[0], numberOfColumns, 23);

    // Check result
    assertEquals(expectedAllColumnsCombination, actualPliManager.allColumnCombination);
  }

  /**
   * Test method for {@link PLIManager#PLIManager(PositionListIndex[], int, int)}
   *
   * The base PLIs should be added to the manager.
   */
  @Test
  public void testConstructorOneColumnCombinationPlisAdded()
      throws ColumnIndexOutOfBoundsException {
    // Setup
    // Expected values
    PositionListIndex[]
        expectedPlis =
        {mock(PositionListIndex.class), mock(PositionListIndex.class)};

    // Execute functionality
    PLIManager pliManager = new PLIManager(expectedPlis, 2, 42);

    // Check result
    assertArrayEquals(expectedPlis, pliManager.basePlis);
  }

  /**
   * Test method for {@link PLIManager#PLIManager(PositionListIndex[], int, Map, int)}
   *
   * The cached PLIs should be added to the manager.
   */
  @Test
  public void testConstructorCachedPlisAdded() throws ColumnIndexOutOfBoundsException {
    // Setup
    PositionListIndex[] basePlis = {mock(PositionListIndex.class), mock(PositionListIndex.class)};
    // Expected values
    Map<ColumnCombinationBitset, PositionListIndex> cachedPlis = new HashMap<>();
    ColumnCombinationBitset columnCombination1 = new ColumnCombinationBitset(0);
    PositionListIndex expectedPli1 = mock(PositionListIndex.class);
    cachedPlis.put(columnCombination1, expectedPli1);
    ColumnCombinationBitset columnCombination2 = new ColumnCombinationBitset(1);
    PositionListIndex expectedPli2 = mock(PositionListIndex.class);
    cachedPlis.put(columnCombination2, expectedPli2);

    // Execute functionality
    PLIManager pliManager = new PLIManager(basePlis, 2, cachedPlis, 42);

    // Check result
    assertEquals(expectedPli1, pliManager.plis.getIfPresent(columnCombination1));
    assertTrue(pliManager.pliSetTrie.getContainedSets().contains(columnCombination1));
    assertEquals(expectedPli2, pliManager.plis.getIfPresent(columnCombination2));
    assertTrue(pliManager.pliSetTrie.getContainedSets().contains(columnCombination2));
  }

  /**
   * Test method for {@link PLIManager#getPli(ColumnCombinationBitset...)}
   */
  @Test
  public void testGetPli() throws PLIBuildingException {
    // Execute functionality
    PositionListIndex actualPli = pliManager.getPli(new ColumnCombinationBitset(0, 1, 2));

    // Check result
    assertEquals(fixture.getExpectedIntersect012(), actualPli);
  }

  /**
   * Test method for {@link PLIManager#getPli(ColumnCombinationBitset...)} )}
   * <p>
   * When column indices are out of bounds an exception should be thrown by the pli manager.
   */
  @Test
  public void testBuildPliIndexOutOfRange() {
    // Setup
    ColumnCombinationBitset invalidColumnCombination = new ColumnCombinationBitset(0, 1, 2,
      3);  // Column index 3 should not be known.

    // Execute functionality
    // Check result
    try {
      pliManager.getPli(invalidColumnCombination);
      fail("Exception should have been thrown.");
    }
    catch (PLIBuildingException e) {
      // Intentionally left blank.
    }
  }

  /**
   * Test method for {@link PLIManager#buildPli(ColumnCombinationBitset)}
   * <p/>
   * The method should return an empty PLI with a compatible number of columns when the PLI for the empty column combination is requested.
   * @throws PLIBuildingException
   */
  @Test
  public void testBuildPliEmptyColumnCombination() throws PLIBuildingException {
    // Setup
    // Expected values
    int expectedPliNumberOfColumns = pliManager.allColumnCombination.size();

    // Execute functionality
    PositionListIndex actualPli = pliManager.buildPli(new ColumnCombinationBitset());

    // Check result
    assertEquals(new PositionListIndex(new ArrayList<IntArrayList>(), expectedPliNumberOfColumns), actualPli);
  }

  /**
   * Test method for {@link PLIManager#buildPli(ColumnCombinationBitset)}
   *
   * The method should add the requested PLI to the cache.
   */
  @Test
  public void testBuildPliAddsRequestedPliToCache() throws PLIBuildingException {
    // Setup
    // Expected values
    ColumnCombinationBitset columnCombination = new ColumnCombinationBitset(0, 1, 2);
    PositionListIndex expectedPli = fixture.getExpectedIntersect012();

    // Check precondition
    assertNull(pliManager.plis.getIfPresent(columnCombination));

    // Execute functionality
    PositionListIndex actualPli = pliManager.buildPli(columnCombination);

    // Check result
    assertNotNull(actualPli);
    assertEquals(expectedPli, actualPli);
  }

  /**
   * Test method for {@link PLIManager#getPli(ColumnCombinationBitset...)}
   */
  @Test
  public void testGetPliMultiple() throws PLIBuildingException {
    // Execute functionality
    PositionListIndex
        actualPli =
        pliManager.getPli(new ColumnCombinationBitset(0, 1), new ColumnCombinationBitset(2));

    // Check result
    assertEquals(fixture.getExpectedIntersect012(), actualPli);
  }

  /**
   * Test method for {@link PLIManager#getPli(ColumnCombinationBitset...)}
   *
   * When getPli is called with no {@link ColumnCombinationBitset}s as argument an {@link
   * PLIBuildingException} should be thrown.
   */
  @Test
  public void testGetPliZero() throws PLIBuildingException {
    // Execute functionality
    // Check result
    try {
      pliManager.getPli();
      fail("Exception should have been thrown.");
    } catch (PLIBuildingException e) {
      // Intentionally left blank.
    }
  }

  /**
   * Test method for {@link PLIManager#getPlis(ColumnCombinationBitset...)}
   */
  @Test
  public void testGetPlis() throws PLIBuildingException {
    // Setup
    // Expected values
    Map<ColumnCombinationBitset, PositionListIndex> expectedPlis = new HashMap<>();
    expectedPlis.put(new ColumnCombinationBitset(1), fixture.getExpectedIntersect1());
    expectedPlis.put(new ColumnCombinationBitset(1, 2), fixture.getExpectedIntersect12());

    // Execute functionality
    Map<ColumnCombinationBitset, PositionListIndex>
        actualPlis = pliManager.getPlis(new ColumnCombinationBitset(1),
                                        new ColumnCombinationBitset(1, 2));

    // Check result
    assertEquals(expectedPlis, actualPlis);
  }

  /**
   * Test method for {@link PLIManager#addPliToCache(ColumnCombinationBitset, PositionListIndex)}
   */
  @Test
  public void testAddPliToCache() throws ColumnIndexOutOfBoundsException, PLIBuildingException {
    // Setup
    PLIManager
        pliManager =
        new PLIManager(new PositionListIndex[0], 10, 42);
    // Expected values
    final ColumnCombinationBitset columnCombinationForLookup = new ColumnCombinationBitset(2, 5, 8);
    final PositionListIndex expectedPli = mock(PositionListIndex.class);

    // Execute functionality
    pliManager.addPliToCache(columnCombinationForLookup, expectedPli);

    // Check result
    assertEquals(expectedPli, pliManager.getPli(columnCombinationForLookup));
    assertTrue(pliManager.pliSetTrie.getContainedSets().contains(columnCombinationForLookup));
  }

  /**
   * Test method for {@link PLIManager#removePliFromCache(ColumnCombinationBitset)}
   */
  @Test
  public void testRemovePliFromCache() {
    // Setup
    final ColumnCombinationBitset columnCombinationToRemove = new ColumnCombinationBitset(2);
    pliManager.plis.put(columnCombinationToRemove, mock(PositionListIndex.class));

    // Check preconditions
    assertNotNull(pliManager.plis.getIfPresent(columnCombinationToRemove));
    assertTrue(pliManager.pliSetTrie.getContainedSets().contains(columnCombinationToRemove));

    // Execute functionality
    pliManager.removePliFromCache(columnCombinationToRemove);
    pliManager.plis.cleanUp();

    // Check result
    assertNull(pliManager.plis.getIfPresent(columnCombinationToRemove));
    assertFalse(pliManager.pliSetTrie.getContainedSets().contains(columnCombinationToRemove));
  }

  /**
   * Test method for {@link PLIManager#close()}
   */
  @Test
  public void testClose() {
    // Check preconditions
    assertFalse(pliManager.exec.isShutdown());

    // Execute functionality
    pliManager.close();

    // Check result
    assertTrue(pliManager.exec.isShutdown());
  }

}
