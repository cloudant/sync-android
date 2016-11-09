/*
 * Copyright (c) 2016 IBM Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.cloudant.sync.internal.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @api_private
 */
public class CollectionUtils {

    public static <E> List<List<E>> partition(List<E> list, int partitionSize) {
        int subLists = (list.size() / partitionSize) + (list.size() % partitionSize == 0 ? 0 : 1);
        ArrayList<List<E>> lists = new ArrayList<List<E>>(subLists);
        for (int i = 0; i < subLists; i++) {
            int minIndex = i * partitionSize;
            int maxIndex = i * partitionSize + partitionSize;
            maxIndex = (maxIndex < list.size()) ? maxIndex : list.size();
            lists.add(Collections.unmodifiableList(list.subList(minIndex, maxIndex)));
        }
        return Collections.unmodifiableList(lists);
    }

    public static <E> List<E> newArrayList(Iterator<E> iterator) {
        List<E> list = new ArrayList<E>();
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return list;
    }
}
