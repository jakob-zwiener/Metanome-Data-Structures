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

import java.util.ArrayList;
import java.util.List;

/**
 * @author Jakob Zwiener
 */
public class AdditionalSetTrieFixture {

  public SetTrie getGraph() throws ColumnIndexOutOfBoundsException {
    SetTrie graph = new SetTrie(getDimension());
    graph.addAll(getExpectedIncludedColumnCombinations());
    return graph;
  }

  public List<ColumnCombinationBitset> getExpectedIncludedColumnCombinations() {
    List<ColumnCombinationBitset> includedColumnCombinations = new ArrayList<>();

    includedColumnCombinations.add(new ColumnCombinationBitset(1, 3, 8));
    includedColumnCombinations.add(new ColumnCombinationBitset(1, 5));
    includedColumnCombinations.add(new ColumnCombinationBitset(1, 10));
    includedColumnCombinations.add(new ColumnCombinationBitset(1, 11, 17));
    includedColumnCombinations.add(new ColumnCombinationBitset(1, 12));
    includedColumnCombinations.add(new ColumnCombinationBitset(7));
    includedColumnCombinations.add(new ColumnCombinationBitset(11));
    includedColumnCombinations.add(new ColumnCombinationBitset(15, 18));
    includedColumnCombinations.add(new ColumnCombinationBitset(27, 28, 29, 30, 31));
    includedColumnCombinations.add(new ColumnCombinationBitset(28, 30));

    return includedColumnCombinations;
  }

  public int getDimension() {
    return 32;
  }

  public ColumnCombinationBitset[] getExpectedSubsetColumnCombinations() {
    return new ColumnCombinationBitset[]{
        new ColumnCombinationBitset(),

        new ColumnCombinationBitset(1),
        new ColumnCombinationBitset(3),
        new ColumnCombinationBitset(8),
        new ColumnCombinationBitset(1, 3),
        new ColumnCombinationBitset(1, 8),
        new ColumnCombinationBitset(3, 8),

        new ColumnCombinationBitset(1),
        new ColumnCombinationBitset(5),

        new ColumnCombinationBitset(1),
        new ColumnCombinationBitset(10),

        new ColumnCombinationBitset(1),
        new ColumnCombinationBitset(11),
        new ColumnCombinationBitset(17),
        new ColumnCombinationBitset(1, 11),
        new ColumnCombinationBitset(1, 17),
        new ColumnCombinationBitset(11, 17),

        new ColumnCombinationBitset(1),
        new ColumnCombinationBitset(12),

        new ColumnCombinationBitset(7),

        new ColumnCombinationBitset(11),

        new ColumnCombinationBitset(15),
        new ColumnCombinationBitset(18),
    };
  }

  public ColumnCombinationBitset[] getExpectedNonSubsetColumnCombinations() {
    return new ColumnCombinationBitset[]{
        new ColumnCombinationBitset(11, 17, 18),
        new ColumnCombinationBitset(1, 7, 15)
    };
  }

  public ColumnCombinationBitset getSubsetCase1() {
    return new ColumnCombinationBitset(1);
  }

  public ColumnCombinationBitset[] getExpectedSupersetsFromQueryCase1() {
    final List<ColumnCombinationBitset>
        expectedSupersets =
        getExpectedIncludedColumnCombinations().subList(0, 5);
    return expectedSupersets.toArray(new ColumnCombinationBitset[expectedSupersets.size()]);
  }

  public ColumnCombinationBitset getSubsetCase2() {
    return new ColumnCombinationBitset(28, 30);
  }

  public ColumnCombinationBitset[] getExpectedSupersetsFromQueryCase2() {
    final List<ColumnCombinationBitset>
        expectedSupersets =
        getExpectedIncludedColumnCombinations().subList(8, 10);
    return expectedSupersets.toArray(new ColumnCombinationBitset[expectedSupersets.size()]);
  }

  public int getContainedSetSubGraphQuery() {
    return 1;
  }

  public ColumnCombinationBitset[] getExpectedContainedSetsSubGraphQuery() {
    final List<ColumnCombinationBitset>
        expectedContainedSets =
        getExpectedIncludedColumnCombinations().subList(0, 5);
    return expectedContainedSets.toArray(new ColumnCombinationBitset[expectedContainedSets.size()]);
  }

}
