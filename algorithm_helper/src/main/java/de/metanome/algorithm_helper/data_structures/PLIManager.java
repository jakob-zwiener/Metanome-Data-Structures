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

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

/**
 * Manages plis and performs intersect operations.
 * @author Jakob Zwiener
 * @see PositionListIndex
 */
public class PLIManager {

  // TODO(zwiener): Make thread pool size accessible from the outside.
  public static transient ListeningExecutorService
      exec =
      MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(4));
  protected List<PositionListIndex> plis;
  protected ColumnCombinationBitset allColumnCombination;

  public PLIManager(final List<PositionListIndex> pliList) {
    plis = pliList;
    int[] allColumnIndices = new int[plis.size()];
    for (int i = 0; i < plis.size(); i++) {
      allColumnIndices[i] = i;
    }
    allColumnCombination = new ColumnCombinationBitset(allColumnIndices);
  }

  public PositionListIndex buildPli(final ColumnCombinationBitset columnCombination) throws PLIBuildingException
  {
    if (!columnCombination.isSubsetOf(allColumnCombination)) {
      throw new PLIBuildingException(
        "The column combination should only contain column indices of plis that are known to the pli manager.");
    }

    long start = System.nanoTime();

    final Queue<ListenableFuture<PositionListIndex>> futures = new LinkedList<>();
    List<Integer> setBits = columnCombination.getSetBits();
    for (int i = 0; i < columnCombination.size() - 1; i += 2) {
      final int leftColumnIndex = setBits.get(i);
      final int rightColumnIndex = setBits.get(i + 1);

      futures.add(exec.submit(new Callable<PositionListIndex>() {
        @Override public PositionListIndex call() throws Exception {
          final PositionListIndex
              intersect =
              plis.get(leftColumnIndex).intersect(plis.get(rightColumnIndex));
          System.out.println(String.format("Raw key error is %d for column combination %s.",
                                           intersect.calculateRawKeyError(),
                                           new ColumnCombinationBitset(leftColumnIndex,
                                                                       rightColumnIndex)
                                               .toString()));
          return intersect;
        }
      }));
    }
    //
    if (columnCombination.size() % 2 != 0) {
      futures.add(exec.submit(new Callable<PositionListIndex>() {
        @Override public PositionListIndex call() throws Exception {
          return plis.get(columnCombination.getSetBits().get(columnCombination.size() - 1));
        }
      }));
    }

    while (futures.size() > 1) {
      ListenableFuture<PositionListIndex> leftPliFuture = futures.remove();
      ListenableFuture<PositionListIndex> rightPliFuture = futures.remove();
      ListenableFuture<List<PositionListIndex>> joinedFuture = Futures.allAsList(leftPliFuture, rightPliFuture);

      // TODO(zwiener): maybe use asyncfunction
      futures.add(Futures.transform(joinedFuture, new Function<List<PositionListIndex>, PositionListIndex>() {
        @Override public PositionListIndex apply(final List<PositionListIndex> input) {
          final PositionListIndex intersect = input.get(0).intersect(input.get(1));
          System.out
              .println(String.format("Raw key error is %d.", intersect.calculateRawKeyError()));

          return intersect;
        }
      }, exec));
    }

    System.out.println((System.nanoTime() - start) / 1000000000d);

    try {
      final PositionListIndex result = futures.remove().get();
      return result;
    }
    catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }

    return null;
  }


}
