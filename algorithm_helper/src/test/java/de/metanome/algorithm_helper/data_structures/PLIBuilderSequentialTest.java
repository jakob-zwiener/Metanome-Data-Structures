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

import java.util.List;

import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.junit.Before;
import org.junit.Test;

import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.InputIterationException;

/**
 * Tests for {@link PLIBuilderSequential}
 * @author Jakob Zwiener
 */
public class PLIBuilderSequentialTest {

  protected PLIBuilderFixture fixture;
  protected PLIBuilderSequential builder;

  @Before
  public void setUp() throws Exception {
    fixture = new PLIBuilderFixture();
    builder = new PLIBuilderSequential(fixture.getInputGenerator());
  }

  /**
   * Test method for {@link PLIBuilderSequential#getPLIList()} <p/> Tests that {@link
   * de.metanome.algorithm_helper.data_structures.PositionListIndex}es are build correctly.
   */
  @Test
  public void testCalculatePLINullEqualsNull() throws PLIBuildingException {
    // Setup
    // Expected values
    List<PositionListIndex> expectedPLIList = fixture.getExpectedPLIList(true);
    PositionListIndex[]
      expectedPLIArray =
      expectedPLIList.toArray(new PositionListIndex[expectedPLIList.size()]);

    // Execute functionality
    List<PositionListIndex> actualPLIList = builder.getPLIList();

    // Check result
    assertThat(actualPLIList, IsIterableContainingInAnyOrder.containsInAnyOrder(expectedPLIArray));
  }

  /**
   * Test method for {@link PLIBuilderSequential#getPLIList()} <p/> Tests that {@link
   * de.metanome.algorithm_helper.data_structures.PositionListIndex}es are build correctly.
   */
  @Test
  public void testCalculatePLINullNotEqualsNull()
    throws PLIBuildingException, InputGenerationException, InputIterationException
  {
    // Setup
    this.builder = new PLIBuilderSequential(fixture.getInputGenerator(), false);
    // Expected values
    List<PositionListIndex> expectedPLIList = fixture.getExpectedPLIList(false);
    PositionListIndex[]
      expectedPLIArray =
      expectedPLIList.toArray(new PositionListIndex[expectedPLIList.size()]);

    // Execute functionality
    List<PositionListIndex> actualPLIList = builder.getPLIList();

    // Check result
    assertThat(actualPLIList, IsIterableContainingInAnyOrder.containsInAnyOrder(expectedPLIArray));
  }
}
