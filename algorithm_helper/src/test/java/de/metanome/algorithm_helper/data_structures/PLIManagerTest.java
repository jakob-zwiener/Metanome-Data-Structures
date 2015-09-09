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

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for {@link PLIManager}
 * @author Jakob Zwiener
 */
public class PLIManagerTest {

  protected PLIManagerFixture fixture;
  protected PLIManager pliManager;

  @Before
  public void setUp() {
    fixture = new PLIManagerFixture();
    pliManager = new PLIManager(fixture.getPlis());
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
  public void testGetPliIndexOutOfRange() {
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

}
