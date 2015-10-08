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

import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

/**
 * Manages plis and performs intersect operations.
 * @author Jakob Zwiener
 * @see PositionListIndex
 */
public class PLIManager {

  public static int N_THREADS = 4;
  // TODO(zwiener): Make thread pool size accessible from the outside.
  public static transient ListeningExecutorService exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(
    N_THREADS));
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

    final MinMaxPriorityQueue<PositionListIndex> priorityQueue = MinMaxPriorityQueue.orderedBy(
      new Comparator<PositionListIndex>() {
        @Override public int compare(final PositionListIndex o1, final PositionListIndex o2) {
          return o1.getRawKeyError() - o2.getRawKeyError();
        }
      }).create();

    for (int columnIndex : columnCombination.getSetBits()) {
      priorityQueue.add(plis.get(columnIndex));
    }

    long sumSync = 0;

    List<ListenableFuture<PositionListIndex>> tasks = new LinkedList<>();
    while (true) {
      if (priorityQueue.size() < 2) {
        break;
      }
      // printQueue(priorityQueue);
      for (int i = 0; i < N_THREADS; i++) {
        if (priorityQueue.size() < 2) {
          break;
        }

        final PositionListIndex leftPli = priorityQueue.poll();

        final PositionListIndex rightPli;

        rightPli = priorityQueue.poll();

        tasks.add(exec.submit(new Callable<PositionListIndex>() {

          @Override public PositionListIndex call() throws Exception {
            PositionListIndex intersect = leftPli.intersect(rightPli);
            /*System.out.println(
                String.format("Raw key error is %d.", intersect.calculateRawKeyError()));*/
            return intersect;
          }
        }));
      }
      long beforeSync = -1;
      for (ListenableFuture<PositionListIndex> task : tasks) {
        try {
          PositionListIndex result = task.get();
          if (beforeSync == -1) {
            beforeSync = System.nanoTime();
          }
          priorityQueue.add(result);
        }
        catch (InterruptedException e) {
          e.printStackTrace();
        }
        catch (ExecutionException e) {
          e.printStackTrace();
        }
      }
      final long syncElapsed = System.nanoTime() - beforeSync;
      // System.out.println(String.format("Sync took: %f", syncElapsed / 1000000000d));
      sumSync += syncElapsed;
      tasks.clear();
    }

    return priorityQueue.poll();
  }

  private void printQueue(final MinMaxPriorityQueue<PositionListIndex> priorityQueue) {
    System.out.println("queue");
    List<PositionListIndex> pliList = new ArrayList<>(priorityQueue);
    pliList.sort(new Comparator<PositionListIndex>() {
      @Override public int compare(final PositionListIndex o1, final PositionListIndex o2) {
        return o1.getRawKeyError() - o2.getRawKeyError();
      }
    });
    for (PositionListIndex pli : pliList) {
      System.out.println(pli.getRawKeyError());
    }
    System.out.println("queue");
  }


}
