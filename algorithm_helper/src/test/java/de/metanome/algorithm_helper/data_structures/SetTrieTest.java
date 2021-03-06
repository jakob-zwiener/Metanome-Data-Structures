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

import de.metanome.test_helper.EqualsAndHashCodeTester;

import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link SetTrie}
 * @author Jens Ehrlich
 * @author Jakob Zwiener
 */
public class SetTrieTest {

  protected SuperSetGraphFixture superSetFixture;
  protected SetTrieFixture subSetFixture;

  @Before
  public void setup() {
    superSetFixture = new SuperSetGraphFixture();
    subSetFixture = new SetTrieFixture();
  }


  /**
   * Test method for {@link SetTrie#add(ColumnCombinationBitset)}
   * <p/> After inserting a column combination a subgraph for every set bit should exist. Add should
   * return the graph after addition.
   */
  @Test
  public void testAdd() throws ColumnIndexOutOfBoundsException {
    // Setup
    // Expected values

    final int expectedDimension = 8;
    SetTrie graph = new SetTrie(expectedDimension);
    ColumnCombinationBitset columnCombination = new ColumnCombinationBitset(2, 4, 7);

    // Execute functionality
    SetTrie graphAfterAdd = graph.add(columnCombination);

    // Check result
    // Check existence of column indices in subgraphs by iterating
    SetTrie actualSubGraph = graph;
    for (int setColumnIndex : columnCombination.getSetBits()) {
      assertEquals(expectedDimension, actualSubGraph.subGraphs.length);
      assertNotNull(actualSubGraph.subGraphs[setColumnIndex]);
      actualSubGraph = actualSubGraph.subGraphs[setColumnIndex];
    }

    // Check add return value
    assertSame(graph, graphAfterAdd);
  }

  /**
   * Test method for {@link SetTrie#add(ColumnCombinationBitset)}
   *
   * The empty set is never returned as superset or subset.
   */
  @Test
  public void testAddEmptyColumnCombination() throws ColumnIndexOutOfBoundsException {
    // Setup
    SetTrie actualGraph = new SetTrie(12);

    // Check precondition
    assertFalse(actualGraph.containsSuperset(new ColumnCombinationBitset()));

    // Execute functionality
    actualGraph.add(new ColumnCombinationBitset());

    // Check result
    assertFalse(actualGraph.containsSuperset(new ColumnCombinationBitset()));
  }

  /**
   * Test method for {@link SetTrie#add(ColumnCombinationBitset)}
   *
   * If the column combination contains a column index that is larger than the dimension of the prefix tree, an exception should be thrown.
   */
  @Test
  public void testAddDimensionTooSmall() {
    // Setup
    SetTrie graph = new SetTrie(12);

    // Execute functionality
    // Check result
    try {
      graph.add(new ColumnCombinationBitset(2, 4, 7, 12));
      fail("Expected exception has not been thrown.");
    } catch (ColumnIndexOutOfBoundsException e) {
      // Intentionally left blank.
    }
  }

  /**
   * Test method for {@link SetTrie#addAll(java.util.Collection)} <p/> After inserting all
   * column combinations the graph should be equal to the expected graph from the superSetFixture. AddAll
   * should return the graph after addition.
   */
  @Test
  public void testAddAll() throws ColumnIndexOutOfBoundsException {
    // Setup
    SetTrie graph = new SetTrie(subSetFixture.getDimension());
    // Expected values
    Collection<ColumnCombinationBitset>
      expectedColumnCombinations =
        subSetFixture.getExpectedIncludedColumnCombinations();
    SetTrie expectedGraph = subSetFixture.getGraph();

    // Execute functionality
    SetTrie graphAfterAddAll = graph.addAll(expectedColumnCombinations);

    // Check result
    assertEquals(expectedGraph, graph);
    assertSame(graph, graphAfterAddAll);
  }

  /**
   * Test method for {@link SetTrie#addAll(Collection)}
   *
   * If one of the column combination contains a column index that is larger than the dimension of the prefix tree, an exception should be thrown. Already added column combination will remain added; column combinations after the violating set will not be added.
   */
  @Test
  public void testAddAllDimensionTooSmall() {
    // Setup
    SetTrie graph = new SetTrie(13);
    // Expected values
    final ColumnCombinationBitset expectedContainedSet = new ColumnCombinationBitset(1, 2, 3);
    final ColumnCombinationBitset expectedNotContainedSet = new ColumnCombinationBitset(7, 8, 9);
    Collection<ColumnCombinationBitset> columnCombinations = new LinkedList<>();
    columnCombinations.add(expectedContainedSet);
    columnCombinations.add(new ColumnCombinationBitset(5, 7, 14));
    columnCombinations.add(expectedNotContainedSet);

    // Execute functionality
    // Check result
    try {
      graph.addAll(columnCombinations);
      fail("Expected exception has not been thrown.");
    } catch (ColumnIndexOutOfBoundsException e) {
      // Intentionally left blank
    }

    final ArrayList<ColumnCombinationBitset> actualContainedSets = graph.getContainedSets();

    assertTrue(actualContainedSets.contains(expectedContainedSet));
    assertFalse(graph.getContainedSets().contains(expectedNotContainedSet));
  }

  /**
   * Test method for {@link SetTrie#getContainedSets()}
   */
  @Test
  public void testGetContainedSets() throws ColumnIndexOutOfBoundsException {
    // Setup
    AdditionalSetTrieFixture fixture = new AdditionalSetTrieFixture();
    SetTrie graph = fixture.getGraph();
    // Expected values
    final List<ColumnCombinationBitset>
        expectedContainedSets =
        fixture.getExpectedIncludedColumnCombinations();

    // Execute functionality
    List<ColumnCombinationBitset> actualContainedSets = graph.getContainedSets();

    // Check result
    assertThat(actualContainedSets,
               IsIterableContainingInAnyOrder
                   .containsInAnyOrder(expectedContainedSets.toArray(
                       new ColumnCombinationBitset[expectedContainedSets
                           .size()])));
  }

  /**
   * Test method for {@link SetTrie#getContainedSets()}
   *
   * For sub graph queries results can be augmented by a result prefix (which represents the path in
   * the super graph).
   */
  @Test
  public void testGetContainedSetsSubGraph() throws ColumnIndexOutOfBoundsException {
    // Setup
    AdditionalSetTrieFixture fixture = new AdditionalSetTrieFixture();
    SetTrie graph = fixture.getGraph();
    // Expected values
    final List<ColumnCombinationBitset>
        expectedContainedSets =
        fixture.getExpectedIncludedColumnCombinations();

    // Execute functionality
    List<ColumnCombinationBitset>
        actualContainedSets =
        graph.subGraphs[fixture.getContainedSetSubGraphQuery()].getContainedSets(
            new ColumnCombinationBitset(fixture.getContainedSetSubGraphQuery()));

    // Check result
    assertThat(actualContainedSets,
               IsIterableContainingInAnyOrder
                   .containsInAnyOrder(fixture.getExpectedContainedSetsSubGraphQuery()));
  }

  /**
   * Test method for {@link SetTrie#getExistingSubsets(ColumnCombinationBitset)}
   */
  @Test
  public void testGetExistingSubsets() throws ColumnIndexOutOfBoundsException {
    // Setup
    SetTrie graph = subSetFixture.getGraph();
    ColumnCombinationBitset
        columnCombinationToQuery =
        subSetFixture.getColumnCombinationForSubsetQuery();

    // Execute functionality
    List<ColumnCombinationBitset>
      actualSubsets =
      graph.getExistingSubsets(columnCombinationToQuery);

    // Check result
    assertThat(actualSubsets,
               IsIterableContainingInAnyOrder
                   .containsInAnyOrder(subSetFixture.getExpectedSubsetsFromQuery()));
  }

  /**
   * Test method for {@link SetTrie#getExistingSubsets(ColumnCombinationBitset)}
   * <p>
   * Tests a special case with an empty graph. An empty list should be returned.
   */
  @Test
  public void testGetExistingSubsetsOnEmptyGraph() {
    // Setup
    SetTrie graph = new SetTrie(6);

    // Execute functionality
    List<ColumnCombinationBitset>
      actualSubsets =
      graph.getExistingSubsets(new ColumnCombinationBitset(1, 3, 5));

    // Check result
    assertTrue(actualSubsets.isEmpty());
  }

  /**
   * Test for the method {@link SetTrie#containsSubset(ColumnCombinationBitset)} )}
   */
  @Test
  public void testContainsSubset() throws ColumnIndexOutOfBoundsException {
    // Setup
    SetTrie actualGraph = subSetFixture.getGraph();

    // Execute functionality
    // Check Result
    assertTrue(
        actualGraph.containsSubset(subSetFixture.getExpectedIncludedColumnCombinations().get(0)));
    assertTrue(actualGraph.containsSubset(subSetFixture.getColumnCombinationForSubsetQuery()));
    assertFalse(actualGraph.containsSubset(new ColumnCombinationBitset(1)));
  }

  /**
   * Test method for {@link SetTrie#containsSubset(ColumnCombinationBitset)}
   * <p>
   * Tests a special case with an empty graph. False should be returned if the graph is empty.
   */
  @Test
  public void testContainsSubsetOnEmptyGraph() {
    //Setup
    SetTrie actualGraph = new SetTrie(4);

    //Execute functionality
    //Check Result
    assertFalse(actualGraph.containsSubset(new ColumnCombinationBitset(1, 3)));
    assertFalse(actualGraph.containsSubset(new ColumnCombinationBitset()));
  }

  /**
   * Test method for {@link SetTrie#containsSubset(ColumnCombinationBitset)}
   *
   * The empty set cannot be added to the graph so it will not be returned as subset of the empty set.
   */
  @Test
  public void testContainsSubsetOnEmptySubset() throws ColumnIndexOutOfBoundsException {
    // Setup
    SetTrie actualGraph = subSetFixture.getGraph();

    // Execute functionality
    assertFalse(actualGraph.containsSubset(new ColumnCombinationBitset()));
  }

  /**
   * Test method for {@link SetTrie#getExistingSupersets(ColumnCombinationBitset)}
   */
  @Test
  public void testGetExistingSupersets() throws ColumnIndexOutOfBoundsException {
    // Setup
    SetTrie graph = superSetFixture.getSetTrie();
    ColumnCombinationBitset
        columnCombinationToQuery =
        superSetFixture.getColumnCombinationForSupersetQuery();

    // Execute functionality
    List<ColumnCombinationBitset>
        actualSubsets =
        graph.getExistingSupersets(columnCombinationToQuery);

    // Check result
    assertThat(actualSubsets,
               IsIterableContainingInAnyOrder
                   .containsInAnyOrder(superSetFixture.getExpectedSupersetsFromQuery()));
  }

  /**
   * Test method for {@link SetTrie#getExistingSupersets(ColumnCombinationBitset)} <p> This
   * tests a special case of an empty graph. An empty list should be returned
   */
  @Test
  public void testGetExistingSupersetsOnEmptyGraph() {
    // Setup
    SetTrie graph = new SetTrie(6);

    // Execute functionality
    List<ColumnCombinationBitset>
        actualSubsets =
        graph.getExistingSupersets(new ColumnCombinationBitset(1, 3, 5));

    // Check result
    assertTrue(actualSubsets.isEmpty());
  }

  /**
   * Test method for {@link SetTrie#getExistingSupersets(ColumnCombinationBitset)}
   */
  @Test
  public void testGetExistingSupersetsAdditionalCase1() throws ColumnIndexOutOfBoundsException {
    // Setup
    AdditionalSetTrieFixture fixture = new AdditionalSetTrieFixture();
    SetTrie actualGraph = fixture.getGraph();

    // Execute functionality
    ArrayList<ColumnCombinationBitset>
        actualSupersets = actualGraph.getExistingSupersets(fixture.getSubsetCase1());

    // Check result
    assertThat(actualSupersets,
               IsIterableContainingInAnyOrder
                   .containsInAnyOrder(fixture.getExpectedSupersetsFromQueryCase1()));
  }

  /**
   * Test method for {@link SetTrie#getExistingSupersets(ColumnCombinationBitset)}
   */
  @Test
  public void testGetExistingSupersetsAdditionalCase2() throws ColumnIndexOutOfBoundsException {
    // Setup
    AdditionalSetTrieFixture fixture = new AdditionalSetTrieFixture();
    SetTrie actualGraph = fixture.getGraph();

    // Execute functionality
    ArrayList<ColumnCombinationBitset>
        actualSupersets = actualGraph.getExistingSupersets(fixture.getSubsetCase2());

    // Check result
    assertThat(actualSupersets,
               IsIterableContainingInAnyOrder
                   .containsInAnyOrder(fixture.getExpectedSupersetsFromQueryCase2()));
  }

  /**
   * Test for the method {@link SetTrie#containsSuperset(ColumnCombinationBitset)}
   */
  @Test
  public void testContainsSuperset() throws ColumnIndexOutOfBoundsException {
    // Setup
    SetTrie actualGraph = superSetFixture.getSetTrie();

    // Execute functionality
    // Check Result
    assertTrue(
        actualGraph
            .containsSuperset(superSetFixture.getExpectedIncludedColumnCombinations().get(0)));
    assertTrue(
        actualGraph.containsSuperset(superSetFixture.getColumnCombinationForSupersetQuery()));
    assertFalse(actualGraph.containsSuperset(new ColumnCombinationBitset(1, 2, 3, 5, 8, 9)));
  }

  /**
   * Test method for {@link SetTrie#containsSuperset(ColumnCombinationBitset)}
   */
  @Test
  public void testContainsSupersetAdditionalCase() throws ColumnIndexOutOfBoundsException {
    // Setup
    AdditionalSetTrieFixture fixture = new AdditionalSetTrieFixture();
    SetTrie actualGraph = fixture.getGraph();

    // Execute functionality
    // Check result
    // All contained columns should also be subsets.
    for (ColumnCombinationBitset expectedSubset : fixture.getExpectedIncludedColumnCombinations()) {
      assertTrue(actualGraph.containsSuperset(expectedSubset));
    }
    for (ColumnCombinationBitset expectedSubset : fixture.getExpectedSubsetColumnCombinations()) {
      assertTrue(actualGraph.containsSuperset(expectedSubset));
    }
    for (ColumnCombinationBitset expectedNonSubsets : fixture
        .getExpectedNonSubsetColumnCombinations()) {
      assertFalse(actualGraph.containsSuperset(expectedNonSubsets));
    }
  }

  /**
   * Test for the method {@link SetTrie#containsSuperset(ColumnCombinationBitset)} <p> This test
   * tests a special case of a empty graph. False should be returned.
   */
  @Test
  public void testContainsSupersetOnEmptyGraph() {
    // Setup
    SetTrie actualGraph = new SetTrie(4);

    // Execute functionality
    // Check Result
    assertFalse(actualGraph.containsSuperset(new ColumnCombinationBitset(1, 3)));
    assertFalse(actualGraph.containsSuperset(new ColumnCombinationBitset()));
  }

  /**
   * Test method for {@link SetTrie#containsSuperset(ColumnCombinationBitset)}
   *
   * All sets in the graph are supersets of the empty set.
   */
  @Test
  public void testContainsSupersetOnEmptySubset() throws ColumnIndexOutOfBoundsException {
    // Setup
    SetTrie actualGraph = superSetFixture.getSetTrie();

    // Execute functionality
    assertTrue(actualGraph.containsSuperset(new ColumnCombinationBitset()));
  }

  /**
   * Test method for {@link SetTrie#isEmpty()}
   */
  @Test
  public void testIsEmpty() throws ColumnIndexOutOfBoundsException {
    // Setup
    SetTrie emptyGraph = new SetTrie(11);
    SetTrie nonEmptyGraph = new SetTrie(11);
    nonEmptyGraph.add(new ColumnCombinationBitset(10));

    // Execute functionality
    // Check result
    assertTrue(emptyGraph.isEmpty());
    assertFalse(nonEmptyGraph.isEmpty());
  }

  /**
   * Test method  {@link SetTrie#equals(Object)} and {@link SetTrie#hashCode()}
   */
  @Test
  public void testEqualsAndHashCode() throws ColumnIndexOutOfBoundsException {
    // Setup
    SetTrie actualGraph = new SetTrie(21);
    SetTrie equalsGraph = new SetTrie(21);
    SetTrie notEqualsGraph = new SetTrie(21);

    actualGraph.add(new ColumnCombinationBitset(2, 5, 10, 20));
    actualGraph.add((new ColumnCombinationBitset(2, 5, 8, 15)));

    equalsGraph.add((new ColumnCombinationBitset(2, 5, 8, 15)));
    equalsGraph.add(new ColumnCombinationBitset(2, 5, 10, 20));

    notEqualsGraph.add(new ColumnCombinationBitset(2, 5, 12, 20));
    notEqualsGraph.add((new ColumnCombinationBitset(2, 5, 10, 15)));

    // Execute functionality
    // Check result
    EqualsAndHashCodeTester<SetTrie> tester = new EqualsAndHashCodeTester<>();
    tester.performBasicEqualsAndHashCodeChecks(actualGraph, equalsGraph, notEqualsGraph);
  }

  /**
   * Test method for {@link SetTrie#getMinimalSubsets()}
   */
  @Test
  public void testGetMinimalSubsets() throws ColumnIndexOutOfBoundsException {
    // Setup
    SetTrie graph = subSetFixture.getGraph();

    // Execute functionality
    Set<ColumnCombinationBitset> actualMinimalSubsets = graph.getMinimalSubsets();

    // Check result
    assertThat(actualMinimalSubsets,
      IsIterableContainingInAnyOrder
          .containsInAnyOrder(subSetFixture.getExpectedMinimalSubsets()));
  }

  /**
   * Test method for {@link SetTrie#toString()}
   */
  @Test
  public void testToString() throws ColumnIndexOutOfBoundsException {
    // Setup
    SetTrie graph = subSetFixture.getGraph();

    // Execute functionality
    String actualStringRepresentation = graph.toString();

    // Check result
    assertEquals(subSetFixture.getExpectedStringRepresentation(), actualStringRepresentation);
  }

  /**
   * Test method for {@link SetTrie#remove(ColumnCombinationBitset)}
   */
  @Test
  public void testRemove() throws ColumnIndexOutOfBoundsException {
    // Setup
    SetTrie actualGraph = subSetFixture.getGraph();

    // Execute functionality
    // Check result
    assertTrue(actualGraph.remove(subSetFixture.columnCombinationToRemove()));

    assertEquals(subSetFixture.expectedGraphAfterRemoval(), actualGraph);
  }

  /**
   * Test method for {@link SetTrie#remove(ColumnCombinationBitset)}
   * <p>
   * The empty column should be successfully removed from any graph.
   */
  @Test
  public void testRemoveEmptyColumnCombination() throws ColumnIndexOutOfBoundsException {
    // Setup
    SetTrie actualGraph = subSetFixture.getGraph();

    // Execute functionality
    // Check result
    assertTrue(actualGraph.remove(new ColumnCombinationBitset()));

    assertEquals(subSetFixture.getGraph(), actualGraph);
  }

  /**
   * Test method for {@link SetTrie#remove(ColumnCombinationBitset)}
   * <p>
   * Trying to remove a column combination from a graph that does not contain the column combination should not alter
   * the graph and return false.
   */
  @Test
  public void testRemoveNotInTheGraph() throws ColumnIndexOutOfBoundsException {
    // Setup
    SetTrie actualGraph = subSetFixture.getGraph();

    // Execute functionality
    // Check result
    assertFalse(actualGraph.remove(new ColumnCombinationBitset(2, 3, 5, 8)));

    assertEquals(subSetFixture.getGraph(), actualGraph);
  }
}
