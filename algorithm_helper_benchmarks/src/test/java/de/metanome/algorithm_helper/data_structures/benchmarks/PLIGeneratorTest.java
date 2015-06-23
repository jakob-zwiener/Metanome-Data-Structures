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

package de.metanome.algorithm_helper.data_structures.benchmarks;

import static org.junit.Assert.*;

import org.junit.Test;

import de.metanome.algorithm_helper.data_structures.PositionListIndex;
import it.unimi.dsi.fastutil.longs.LongArrayList;

/**
 * Tests for {@link PLIGenerator}
 *
 * @author Jakob Zwiener
 */
public class PLIGeneratorTest {

  /**
   * Test for {@link PLIGenerator#generatePli(long, int)}
   *
   * Plis should be generated in the correct sizes.
   */
  @Test
  public void testGeneratePLISizes() {
    // Setup
    // Expected values
    final int expectedNumberOfClusters = 3;
    final int expectedClusterSize = 100;

    // Execute functionality
    PositionListIndex
        actualPli =
        PLIGenerator.generatePli(expectedClusterSize, expectedNumberOfClusters);

    // Check result
    assertEquals(expectedNumberOfClusters, actualPli.getClusters().size());
    for (LongArrayList cluster : actualPli.getClusters()) {
      assertEquals(expectedClusterSize, cluster.size());
    }
  }

  /**
   * Test for {@link PLIGenerator#generatePli(long, int)}
   *
   * Generated plis should be different from each other.
   */
  @Test
  public void testGeneratePLIRandom() {
    // Setup
    final int clusterSize = 100;
    final int numberOfClusters = 42;

    // Execute functionality
    PositionListIndex firstPli = PLIGenerator.generatePli(clusterSize, numberOfClusters);
    PositionListIndex secondPli = PLIGenerator.generatePli(clusterSize, numberOfClusters);

    // Check result
    assertNotEquals(firstPli, secondPli);
  }

}
