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
 * @author Jens Ehrlich
 */
public class SuperSetGraphFixture {

  public SetTrie getSetTrie() throws ColumnIndexOutOfBoundsException {
    SetTrie graph = new SetTrie(getNumberOfColumns());
    graph.addAll(getExpectedIncludedColumnCombinations());
    return graph;
  }

  public List<ColumnCombinationBitset> getExpectedIncludedColumnCombinations() {
    List<ColumnCombinationBitset> includedColumnCombinations = new ArrayList<>();

    includedColumnCombinations.add(new ColumnCombinationBitset(1, 4, 6, 8));
    includedColumnCombinations.add(new ColumnCombinationBitset(1, 3, 4, 6));
    includedColumnCombinations.add(new ColumnCombinationBitset(1, 2, 4, 7));
    includedColumnCombinations.add(new ColumnCombinationBitset(1, 3));
    includedColumnCombinations.add(getColumnCombinationToRemove());
    includedColumnCombinations.add(new ColumnCombinationBitset(5, 6, 8));

    return includedColumnCombinations;
  }

  public int getNumberOfColumns() {
    return 11;
  }

  public ColumnCombinationBitset getColumnCombinationForSupersetQuery() {
    return new ColumnCombinationBitset(1, 3);
  }

  public ColumnCombinationBitset[] getExpectedSupersetsFromQuery() {
    return new ColumnCombinationBitset[] {
      getExpectedIncludedColumnCombinations().get(1),
      getExpectedIncludedColumnCombinations().get(3),
      getExpectedIncludedColumnCombinations().get(4)
    };
  }

  public ColumnCombinationBitset getColumnCombinationToRemove() {
    return new ColumnCombinationBitset(1, 2, 3, 4, 7, 8);
  }

}
