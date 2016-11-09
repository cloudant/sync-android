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

package com.cloudant.sync.util;

import static org.junit.Assert.assertEquals;

import com.cloudant.sync.internal.util.CollectionUtils;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class CollectionUtilsTest {

    static List<Integer> INTS_1_to_100 = getInts(1, 100);

    @Test
    public void arrayListFromIterator() throws Exception {
        assertEquals("The lists should be the same", INTS_1_to_100, CollectionUtils.newArrayList
                (INTS_1_to_100.iterator()));
    }

    @Test
    public void equalPartitions() throws Exception {
        List<Integer> ints1to50 = getInts(1, 50);
        List<Integer> ints51to100 = getInts(51, 100);
        List<List<Integer>> intParts = CollectionUtils.partition(INTS_1_to_100, 50);
        assertEquals("There should be two partitions", 2, intParts.size());
        List<Integer> part1 = intParts.get(0);
        List<Integer> part2 = intParts.get(1);
        assertEquals("Part 1 should be first 50", ints1to50, part1);
        assertEquals("Part 2 should be second 50", ints51to100, part2);
    }

    @Test
    public void unequalPartitions() throws Exception {
        List<List<Integer>> intParts = CollectionUtils.partition(getInts(1,3), 2);
        assertEquals("There should be two partitions", 2, intParts.size());
        List<Integer> part1 = intParts.get(0);
        List<Integer> part2 = intParts.get(1);
        assertEquals("Part 1 should be 1,2", getInts(1,2), part1);
        assertEquals("Part 2 should be 3", getInts(3,3), part2);
    }

    @Test
    public void morePartitions() throws Exception {
        List<List<Integer>> intParts = CollectionUtils.partition(INTS_1_to_100, 8);
        assertEquals("There should be thirteen partitions", 13, intParts.size());
        int start = 1;
        for (List<Integer> part : intParts) {
            int end = start + part.size() - 1;
            assertEquals(getInts(start, end), part);
            start = end + 1;
        }
    }

    /**
     * Generate a list of integers
     *
     * @param m start
     * @param n end (inclusive)
     * @return a list of the integers from m to n inclusive
     */
    private static List<Integer> getInts(int m, int n) {
        List<Integer> ints = new ArrayList<Integer>((n - m) + 1);
        for (int i = m; i <= n; i++) {
            ints.add(i);
        }
        return ints;
    }
}
