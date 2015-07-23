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

import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link MaterializedPLI}
 * @author Jakob Zwiener
 */
public class MaterializedPLITest {

  protected PositionListIndexFixture fixture;

  @Before
  public void setUp() throws Exception {
    fixture = new PositionListIndexFixture();
  }

  /**
   * Test method for {@link MaterializedPLI#MaterializedPLI(PositionListIndex)} and {@link MaterializedPLI#asArray(PositionListIndex)}
   */
  @Test
  public void testConstructor() {
    // Setup
    PositionListIndex firstPli = fixture.getFirstPLI();
    // Expected values
    int[] expectedList = fixture.getFirstPLIAsArray();

    // Execute functionality
    MaterializedPLI actualMaterializedPli = new MaterializedPLI(firstPli);

    assertArrayEquals(expectedList, actualMaterializedPli.dataRepresentatives);
  }

  /**
   * Test method for {@link MaterializedPLI#getNumberOfRows()}
   */
  @Test
  public void testGetNumberOfRows() {
    // Setup
    PositionListIndex firstPli = fixture.getFirstPLI();
    MaterializedPLI materializedFirstPli = new MaterializedPLI(firstPli);

    // Execute functionality
    int actualNumberOfRows = materializedFirstPli.getNumberOfRows();

    // Check result
    assertEquals(fixture.getExpectedFirstNumberOfRows(), actualNumberOfRows);
  }
}
