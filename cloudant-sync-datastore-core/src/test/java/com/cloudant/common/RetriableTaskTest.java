/*
 * Copyright © 2013 Cloudant, Inc. All rights reserved.
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

package com.cloudant.common;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Callable;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.cloudant.sync.internal.common.RetriableTask;

public class RetriableTaskTest {

    @Test
    public void call_noFailure() throws Exception {
        TestCallable task = new TestCallable(0, 1);

        RetriableTask<Integer> retriableTask = new RetriableTask<Integer>(task);
        Assert.assertTrue(1 == retriableTask.call());

        Assert.assertEquals(3, retriableTask.getTotalTries());
        Assert.assertEquals(3, retriableTask.getTriesRemaining());
    }

    @Test
    public void call_oneFailure() throws Exception {
        TestCallable task = new TestCallable(1, 1);

        RetriableTask<Integer> retriableTask = new RetriableTask<Integer>(task);
        Assert.assertTrue(1 == retriableTask.call());

        Assert.assertEquals(3, retriableTask.getTotalTries());
        Assert.assertEquals(2, retriableTask.getTriesRemaining());
    }

    @Test
    public void call_twoFailure() throws Exception {
        TestCallable task = new TestCallable(2, 1);

        RetriableTask<Integer> retriableTask = new RetriableTask<Integer>(task);
        Assert.assertTrue(1 == retriableTask.call());

        Assert.assertEquals(3, retriableTask.getTotalTries());
        Assert.assertEquals(1, retriableTask.getTriesRemaining());
    }

    @Test(expected = Exception.class)
    public void call_threeFailure() throws Exception {
        TestCallable task = new TestCallable(3, 1);

        RetriableTask<Integer> retriableTask = new RetriableTask<Integer>(task);
        retriableTask.call();
    }

    @Test(expected = InterruptedException.class)
    public void call_interruptException() throws Exception {
        Callable<Integer> task = mock(TestCallable.class);

        when(task.call()).thenThrow(new InterruptedException());

        RetriableTask<Integer> retriableTask = new RetriableTask<Integer>(task);
        retriableTask.call();
    }

    public class TestCallable implements Callable<Integer>  {

        private final int numFailures;
        private final int result;

        private int numTries = 0;

        public TestCallable(int numFailures, int result) {
            this.numFailures = numFailures;
            this.result = result;
        }

        @Override
        public Integer call() throws Exception {
            numTries++;
            if(numTries <= numFailures) {
                throw new Exception("Some random failure");
            } else {
                return this.result;
            }
        }
    }

}
