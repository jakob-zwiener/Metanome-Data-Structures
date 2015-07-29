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

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.junit.Test;

import de.metanome.test_helper.EqualsAndHashCodeTester;

/**
 * Tests for {@link de.metanome.algorithm_helper.data_structures.SubSetGraph}
 * @author Jens Ehrlich
 * @author Jakob Zwiener
 */
public class SubSetGraphTest {

  /**
   * Test method for {@link de.metanome.algorithm_helper.data_structures.SubSetGraph#add(ColumnCombinationBitset)}
   * <p/> After inserting a column combination a subgraph for every set bit should exist. Add should
   * return the graph after addition.
   */
  @Test
  public void testAdd() {
    // Setup
    SubSetGraph graph = new SubSetGraph();
    ColumnCombinationBitset columnCombination = new ColumnCombinationBitset(2, 4, 7);

    // Execute functionality
    SubSetGraph graphAfterAdd = graph.add(columnCombination);

    // Check result
    // Check existence of column indices in subgraphs by iterating
    SubSetGraph actualSubGraph = graph;
    for (int setColumnIndex : columnCombination.getSetBits()) {
      assertTrue(actualSubGraph.subGraphs.containsKey(setColumnIndex));
      actualSubGraph = actualSubGraph.subGraphs.get(setColumnIndex);
    }

    // Check add return value
    assertSame(graph, graphAfterAdd);
  }

  /**
   * Test method for {@link SubSetGraph#addAll(java.util.Collection)} <p/> After inserting all
   * column combinations the graph should be equal to the expected graph from the fixture. AddAll
   * should return the graph after addition.
   */
  @Test
  public void testAddAll() {
    // Setup
    SubSetGraphFixture fixture = new SubSetGraphFixture();
    SubSetGraph graph = new SubSetGraph();
    // Expected values
    Collection<ColumnCombinationBitset>
      expectedColumnCombinations =
      fixture.getExpectedIncludedColumnCombinations();
    SubSetGraph expectedGraph = fixture.getGraph();

    // Execute functionality
    SubSetGraph graphAfterAddAll = graph.addAll(expectedColumnCombinations);

    // Check result
    assertEquals(expectedGraph, graph);
    assertSame(graph, graphAfterAddAll);
  }

  /**
   * Test method for {@link SubSetGraph#getExistingSubsets(ColumnCombinationBitset)}
   */
  @Test
  public void testGetExistingSubsets() {
    // Setup
    SubSetGraphFixture fixture = new SubSetGraphFixture();
    SubSetGraph graph = fixture.getGraph();
    ColumnCombinationBitset columnCombinationToQuery = fixture.getColumnCombinationForSubsetQuery();

    // Execute functionality
    List<ColumnCombinationBitset>
      actualSubsets =
      graph.getExistingSubsets(columnCombinationToQuery);

    // Check result
    assertThat(actualSubsets,
      IsIterableContainingInAnyOrder
        .containsInAnyOrder(fixture.getExpectedSubsetsFromQuery()));
  }

  /**
   * Test method for {@link SubSetGraph#getExistingSubsets(ColumnCombinationBitset)}
   * <p>
   * Tests a special case with an empty graph. An empty list should be returned.
   */
  @Test
  public void testGetExistingSubsetsOnEmptyGraph() {
    // Setup
    SubSetGraph graph = new SubSetGraph();

    // Execute functionality
    List<ColumnCombinationBitset>
      actualSubsets =
      graph.getExistingSubsets(new ColumnCombinationBitset(1, 3, 5));

    // Check result
    assertTrue(actualSubsets.isEmpty());
  }

  /**
   * Test for the method {@link SubSetGraph#containsSubset(ColumnCombinationBitset)} )}
   */
  @Test
  public void testContainsSubset() {
    //Setup
    SubSetGraphFixture fixture = new SubSetGraphFixture();
    SubSetGraph actualGraph = fixture.getGraph();

    //Execute functionality
    assertTrue(actualGraph.containsSubset(fixture.getExpectedIncludedColumnCombinations().get(0)));
    assertTrue(actualGraph.containsSubset(fixture.getColumnCombinationForSubsetQuery()));
    assertFalse(actualGraph.containsSubset(new ColumnCombinationBitset(1)));
    //Check Result

  }

  /**
   * Test for the method {@link SubSetGraph#containsSubset(ColumnCombinationBitset)}
   * <p>
   * Tests a special case with an empty graph. False should be returned if the graph is empty.
   */
  @Test
  public void testContainsSubsetOnEmptyGraph() {
    //Setup
    SubSetGraph actualGraph = new SubSetGraph();

    //Execute functionality
    assertFalse(actualGraph.containsSubset(new ColumnCombinationBitset(1, 3)));
    //Check Result
  }

  /**
   * Test method for {@link SubSetGraph#isEmpty()}
   */
  @Test
  public void testIsEmpty() {
    // Setup
    SubSetGraph emptyGraph = new SubSetGraph();
    SubSetGraph nonEmptyGraph = new SubSetGraph();
    nonEmptyGraph.add(new ColumnCombinationBitset(10));

    // Execute functionality
    // Check result
    assertTrue(emptyGraph.isEmpty());
    assertFalse(nonEmptyGraph.isEmpty());
  }

  /**
   * Test method  {@link SubSetGraph#equals(Object)} and {@link SubSetGraph#hashCode()}
   */
  @Test
  public void testEqualsAndHashCode() {
    // Setup
    SubSetGraph actualGraph = new SubSetGraph();
    SubSetGraph equalsGraph = new SubSetGraph();
    SubSetGraph notEqualsGraph = new SubSetGraph();

    actualGraph.add(new ColumnCombinationBitset(2, 5, 10, 20));
    actualGraph.add((new ColumnCombinationBitset(2, 5, 8, 15)));

    equalsGraph.add((new ColumnCombinationBitset(2, 5, 8, 15)));
    equalsGraph.add(new ColumnCombinationBitset(2, 5, 10, 20));

    notEqualsGraph.add(new ColumnCombinationBitset(2, 5, 12, 20));
    notEqualsGraph.add((new ColumnCombinationBitset(2, 5, 10, 15)));

    // Execute functionality
    // Check result
    EqualsAndHashCodeTester<SubSetGraph> tester = new EqualsAndHashCodeTester<>();
    tester.performBasicEqualsAndHashCodeChecks(actualGraph, equalsGraph, notEqualsGraph);
  }

  /**
   * Test method for {@link SubSetGraph#getMinimalSubsets()}
   */
  @Test
  public void testGetMinimalSubsets() {
    // Setup
    SubSetGraphFixture fixture = new SubSetGraphFixture();
    SubSetGraph graph = fixture.getGraph();

    // Execute functionality
    Set<ColumnCombinationBitset> actualMinimalSubsets = graph.getMinimalSubsets();

    // Check result
    assertThat(actualMinimalSubsets,
      IsIterableContainingInAnyOrder
        .containsInAnyOrder(fixture.getExpectedMinimalSubsets()));
  }

  /**
   * Test method for {@link SubSetGraph#toString()}
   */
  @Test
  public void testToString() {
    // Setup
    SubSetGraphFixture fixture = new SubSetGraphFixture();
    SubSetGraph graph = fixture.getGraph();

    // Execute functionality
    String actualStringRepresentation = graph.toString();

    // Check result
    assertEquals(fixture.getExpectedStringRepresentation(), actualStringRepresentation);
  }

  /**
   * Test method for {@link SubSetGraph#remove(ColumnCombinationBitset)}
   */
  @Test
  public void testRemove() {
    // Setup
    SubSetGraphFixture fixture = new SubSetGraphFixture();
    SubSetGraph actualGraph = fixture.getGraph();

    // Execute functionality
    // Check result
    assertTrue(actualGraph.remove(fixture.columnCombinationToRemove()));

    assertEquals(fixture.expectedGraphAfterRemoval(), actualGraph);
  }

  /**
   * Test method for {@link SubSetGraph#remove(ColumnCombinationBitset)}
   * <p>
   * The empty column should be successfully removed from any graph.
   */
  @Test
  public void testRemoveEmptyColumnCombination() {
    // Setup
    SubSetGraphFixture fixture = new SubSetGraphFixture();
    SubSetGraph actualGraph = fixture.getGraph();

    // Execute functionality
    // Check result
    assertTrue(actualGraph.remove(new ColumnCombinationBitset()));

    assertEquals(fixture.getGraph(), actualGraph);
  }

  /**
   * Test method for {@link SubSetGraph#remove(ColumnCombinationBitset)}
   * <p>
   * Trying to remove a column combination from a graph that does not contain the column combination should not alter
   * the graph and return false.
   */
  @Test
  public void testRemoveNotInTheGraph() {
    // Setup
    SubSetGraphFixture fixture = new SubSetGraphFixture();
    SubSetGraph actualGraph = fixture.getGraph();

    // Execute functionality
    // Check result
    assertFalse(actualGraph.remove(new ColumnCombinationBitset(2, 3, 5, 8)));

    assertEquals(fixture.getGraph(), actualGraph);
  }
}
