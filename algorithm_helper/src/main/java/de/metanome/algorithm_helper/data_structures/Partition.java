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

/**
 * An interface for index structures representing the equivalence classes of equal attributes.
 * @author Jakob Zwiener
 * @see PositionListIndex
 * @see MaterializedPLI
 */
public interface Partition {

  int SINGLETON_VALUE = 0;

  PositionListIndex intersect(PositionListIndex otherPli);

  int getNumberOfRows();

}
