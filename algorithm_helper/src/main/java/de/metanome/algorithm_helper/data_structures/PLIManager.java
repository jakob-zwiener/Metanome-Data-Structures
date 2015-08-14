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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;

import it.unimi.dsi.fastutil.ints.IntArrayList;

/**
 * Manages plis and performs intersect operations.
 * @author Jakob Zwiener
 * @see PositionListIndex
 */
public class PLIManager {

  protected LoadingCache<ColumnCombinationBitset, PositionListIndex> plis;
  protected Map<ColumnCombinationBitset, PositionListIndex> basePlis;
  protected ColumnCombinationBitset allColumnCombination;
  protected SubSetGraph pliGraph;

  public PLIManager(final Map<ColumnCombinationBitset, PositionListIndex> basePlis) {

    // TODO(zwiener): maxWeight parameter should be settable from outside.
    Runtime runtime = Runtime.getRuntime();

    System.out.println(String.format("max memory in kb: %d", runtime.maxMemory() * 5 / 1024 / 10));

    this.plis = CacheBuilder.newBuilder()
      .weigher(new Weigher<ColumnCombinationBitset, PositionListIndex>() {
        @Override public int weigh(final ColumnCombinationBitset key, final PositionListIndex value) {
          long bytesColumnCombination = key.bitset.getBits().length * 8;
          long bytesPli = 0;

          for (IntArrayList cluster : value.getClusters()) {
            bytesPli += cluster.size();
          }

          bytesPli = bytesPli * 4;

          // Over allocation
          int kBytesAll = (int) ((bytesColumnCombination + bytesPli) * 2 / 1024);

          return kBytesAll;
        }
      })
      .maximumWeight(runtime.maxMemory() * 4 / 1024 / 10)
      .removalListener(new RemovalListener<ColumnCombinationBitset, PositionListIndex>() {
        @Override
        public void onRemoval(final RemovalNotification<ColumnCombinationBitset, PositionListIndex> notification) {
          if (notification.getCause() != RemovalCause.REPLACED) {
            System.out.println("removed: " + notification.getKey() + " " + notification.getCause());
          }
        }
      })
      .build(new CacheLoader<ColumnCombinationBitset, PositionListIndex>() {
        @Override public PositionListIndex load(final ColumnCombinationBitset key) throws Exception {
          return buildPli(key);
        }
      });

    this.plis.putAll(basePlis);

    this.basePlis = basePlis;

    pliGraph = new SubSetGraph();
    pliGraph.addAll(plis.keySet());

    // TODO(zwiener): Number of columns should be independent of pli map.
    int[] allColumnIndices = new int[basePlis.size()];
    for (int i = 0; i < basePlis.size(); i++) {
      allColumnIndices[i] = i;
    }
    allColumnCombination = new ColumnCombinationBitset(allColumnIndices);
  }

  public PLIManager(final Map<ColumnCombinationBitset, PositionListIndex> basePlis,
                    LoadingCache<ColumnCombinationBitset, PositionListIndex> cachedPlis)
  {
    this(basePlis);

    this.plis = cachedPlis;
  }

  public PositionListIndex getPli(ColumnCombinationBitset columnCombination) throws ExecutionException {
    return plis.get(columnCombination);
  }

  public PositionListIndex getPli(ColumnCombinationBitset left, ColumnCombinationBitset right)
    throws ExecutionException
  {
    // TODO(zwiener): Union could be checked.
    return plis.get(left).intersect(plis.get(right));
  }

  protected PositionListIndex buildPli(final ColumnCombinationBitset columnCombination)
    throws PLIBuildingException, ExecutionException
  {
    if (!columnCombination.isSubsetOf(allColumnCombination)) {
      throw new PLIBuildingException(
        "The column combination should only contain column indices of plis that are known to the pli manager.");
    }

    PositionListIndex exactPli = plis.get(columnCombination);
    if (exactPli != null) {
      return exactPli;
    }

    List<ColumnCombinationBitset> subsets = pliGraph.getExistingSubsets(columnCombination);

    // Calculate set cover.
    ColumnCombinationBitset unionSoFar = new ColumnCombinationBitset();
    ColumnCombinationBitset bestSubset = new ColumnCombinationBitset();
    ColumnCombinationBitset bestUnion = new ColumnCombinationBitset();
    List<ColumnCombinationBitset> solution = new LinkedList<>();
    while (!unionSoFar.equals(columnCombination)) {
      for (ColumnCombinationBitset subset : subsets) {

        ColumnCombinationBitset currentUnion = unionSoFar.union(subset);

        if (currentUnion.size() > bestUnion.size()) {
          bestSubset = subset;
          bestUnion = currentUnion;
        }
        else if (currentUnion.size() == bestUnion.size()) {
          // If multiple subsets add the same number of columns use key error as tiebreaker.
          if (plis.get(bestSubset).getRawKeyError() > plis.get(subset).getRawKeyError()) {
            bestSubset = subset;
            bestUnion = currentUnion;
          }
        }
      }
      solution.add(bestSubset);
      unionSoFar = unionSoFar.union(bestSubset);
    }

    // Perform the necessary intersections.
    PositionListIndex intersect = null;
    ColumnCombinationBitset unionCombination = null;
    for (ColumnCombinationBitset subsetCombination : solution) {
      if (intersect == null) {
        intersect = plis.get(subsetCombination);
        unionCombination = subsetCombination;
        continue;
      }

      intersect = intersect.intersect(plis.get(subsetCombination));
      unionCombination = unionCombination.union(subsetCombination);
      plis.put(unionCombination, intersect);
      pliGraph.add(unionCombination);
    }

    return intersect;
  }


}
