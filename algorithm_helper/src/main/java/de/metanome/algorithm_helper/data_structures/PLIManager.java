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

import java.util.HashMap;
import java.util.Map;

/**
 * Manages plis and performs intersect operations.
 * @author Jakob Zwiener
 * @see PositionListIndex
 */
public class PLIManager {

  protected Map<ColumnCombinationBitset, PositionListIndex> plis;
  protected Map<ColumnCombinationBitset, MaterializedPLI> materializedPlis;
  protected ColumnCombinationBitset allColumnCombination;

  public PLIManager(final Map<ColumnCombinationBitset, PositionListIndex> pliList) {
    plis = pliList;
    materializedPlis = new HashMap<>();
    int[] allColumnIndices = new int[plis.size()];
    for (int i = 0; i < plis.size(); i++) {
      allColumnIndices[i] = i;
    }
    allColumnCombination = new ColumnCombinationBitset(allColumnIndices);
  }

  public void pinPlis(ColumnCombinationBitset... columnCombinations) {
    // TODO(zwiener): test
    for (ColumnCombinationBitset columnCombination : columnCombinations) {
      materializedPlis.put(columnCombination, new MaterializedPLI(plis.get(columnCombination)));
    }
  }

  public void unpinPlis(ColumnCombinationBitset... columnCombinations) {
    // TODO(zwiener): test
    for (ColumnCombinationBitset columnCombination : columnCombinations) {
      materializedPlis.remove(columnCombination);
    }
  }

  public PositionListIndex buildPli(final ColumnCombinationBitset columnCombination) throws PLIBuildingException {
    if (!columnCombination.isSubsetOf(allColumnCombination)) {
      throw new PLIBuildingException(
        "The column combination should only contain column indices of plis that are known to the pli manager.");
    }

    PositionListIndex pli = plis.get(columnCombination);

    if (pli != null) {
      return pli;
    }


    PositionListIndex intersect = null;
    Partition firstPartition = null;
    ColumnCombinationBitset unionColumnCombination = null;
    for (ColumnCombinationBitset oneColumnCombination : columnCombination.getContainedOneColumnCombinations()) {
      if (intersect == null) {
        if (firstPartition == null) {
          firstPartition = getPartition(oneColumnCombination);
          unionColumnCombination = oneColumnCombination;
          continue;
        }
        intersect = firstPartition.intersect(plis.get(oneColumnCombination));
        unionColumnCombination = unionColumnCombination.union(oneColumnCombination);
        plis.put(unionColumnCombination, intersect);
        continue;
      }
      intersect = intersect.intersect(getPartition(oneColumnCombination));
      unionColumnCombination = unionColumnCombination.union(oneColumnCombination);
      plis.put(unionColumnCombination, intersect);
    }

    return intersect;

  }

  public PositionListIndex buildPli(ColumnCombinationBitset left, ColumnCombinationBitset right)
    throws PLIBuildingException
  {
    Partition leftPartition = getPartition(left);
    Partition rightPartition = getPartition(right);

    if (leftPartition == null) {
      leftPartition = buildPli(left);
    }

    if (rightPartition == null) {
      rightPartition = buildPli(right);
    }

    if (leftPartition.getRawKeyError() > rightPartition.getRawKeyError()) {
      return leftPartition.intersect(plis.get(right));
    }
    else {
      return rightPartition.intersect(plis.get(left));
    }


    /*MaterializedPLI leftMaterialized = materializedPlis.get(left);
    MaterializedPLI rightMaterialized = materializedPlis.get(right);
    PositionListIndex leftPli = plis.get(left);
    PositionListIndex rightPli = plis.get(right);

    if ((leftMaterialized == null) && (rightMaterialized == null)) {
      if ((leftPli == null) && (rightPli == null)) {
        return buildPli(left.union(right));
      }
      else if ((leftPli != null) && (rightPli == null)) {
        return leftPli.intersect(buildPli(right));
      }
      else if ((leftPli == null) && (rightPli != null)) {
        return buildPli(left).intersect(rightPli);
      }
      else {
        return leftPli.intersect(rightPli);
      }
    }
    else if ((leftMaterialized != null) && (rightMaterialized == null)) {
      System.out.println("materialization saved");
      if (rightPli == null) {
        return leftMaterialized.intersect(buildPli(right));
      }
      else {
        return leftMaterialized.intersect(rightPli);
      }
    }
    else if ((leftMaterialized == null) && (rightMaterialized != null)) {
      System.out.println("materialization saved");
      if (leftPli == null) {
        return rightMaterialized.intersect(buildPli(left));
      }
      else {
        return rightMaterialized.intersect(leftPli);
      }
    }
    else {
      // TODO(zwiener): Choose materialized pli depending on key error.
      System.out.println("materialization saved");
      if (leftPli == null) {
        return rightMaterialized.intersect(buildPli(left));
      }
      else {
        return rightMaterialized.intersect(leftPli);
      }
    }*/
  }

  protected Partition getPartition(ColumnCombinationBitset columnCombination) {
    // TODO(zwiener): test
    // TODO(zwiener): check that this is always used
    Partition partition = materializedPlis.get(columnCombination);

    if (partition == null) {
      partition = plis.get(columnCombination);
    }

    return partition;
  }


}
