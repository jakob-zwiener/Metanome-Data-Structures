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

public class SubSetGraphFixture {

  public SubSetGraph getGraph() {
    SubSetGraph graph = new SubSetGraph();

    for (ColumnCombinationBitset columnCombination : getExpectedIncludedColumnCombinations()) {
      graph.add(columnCombination);
    }

    return graph;
  }

  public List<ColumnCombinationBitset> getExpectedIncludedColumnCombinations() {
    List<ColumnCombinationBitset> includedColumnCombinations = new ArrayList<>();

    includedColumnCombinations.add(new ColumnCombinationBitset(1, 5, 6, 11));
    includedColumnCombinations.add(new ColumnCombinationBitset(1, 3, 4, 6));
    includedColumnCombinations.add(new ColumnCombinationBitset(1, 2, 4, 7));
    includedColumnCombinations.add(new ColumnCombinationBitset(1, 3));
    includedColumnCombinations.add(new ColumnCombinationBitset(2, 3, 4, 7, 11));
    includedColumnCombinations.add(new ColumnCombinationBitset(5, 6, 11));

    return includedColumnCombinations;
  }

  public ColumnCombinationBitset getColumnCombinationForSubsetQuery() {
    return new ColumnCombinationBitset(1, 2, 3, 4, 5, 6, 11);
  }

  public ColumnCombinationBitset[] getExpectedSubsetsFromQuery() {
    return new ColumnCombinationBitset[] {
      getExpectedIncludedColumnCombinations().get(0),
      getExpectedIncludedColumnCombinations().get(1),
      getExpectedIncludedColumnCombinations().get(3),
      getExpectedIncludedColumnCombinations().get(5)
    };
  }

  public ColumnCombinationBitset[] getExpectedMinimalSubsets() {
    return new ColumnCombinationBitset[] {
      getExpectedIncludedColumnCombinations().get(2),
      getExpectedIncludedColumnCombinations().get(3),
      getExpectedIncludedColumnCombinations().get(4),
      getExpectedIncludedColumnCombinations().get(5)
    };
  }

  public String getExpectedStringRepresentation() {
    return
      "1      2  5\n"
        + "2 3 5  3  6\n"
        + "4 4 6  4  11\n"
        + "7 6 11 7\n"
        + "       11";
  }
}
