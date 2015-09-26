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

import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

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
    pliManager = new PLIManager(fixture.getPlis(), fixture.numberOfColumns());
  }

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
        new PLIManager(new HashMap<ColumnCombinationBitset, PositionListIndex>(), numberOfColumns);

    // Check result
    assertEquals(expectedAllColumnsCombination, actualPliManager.allColumnCombination);
  }

  // TODO(zwiener): Check that given plis are added to the cache and the trie.

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
        new PLIManager(new HashMap<ColumnCombinationBitset, PositionListIndex>(), 10);
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

    // Check preconditions
    assertNotNull(pliManager.plis.get(columnCombinationToRemove));
    assertTrue(pliManager.pliSetTrie.getContainedSets().contains(columnCombinationToRemove));

    // Execute functionality
    pliManager.removePliFromCache(columnCombinationToRemove);

    // Check result
    assertNull(pliManager.plis.get(columnCombinationToRemove));
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
