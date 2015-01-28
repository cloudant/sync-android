/**
 * Copyright (c) 2015 Cloudant, Inc. All rights reserved.
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

package cloudant.com.androidtest;

import android.util.Log;

import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by Rhys Short on 28/08/2014.
 */
public class CloudantListener extends RunListener {


    private TestResults currentTest;

    private List<TestResults> competedTests = new LinkedList<TestResults>();


    @Override
    public void testFailure(Failure failure) {
        Log.i("Test Failed",failure.getDescription().getDisplayName());

        // filter out test using initializationError since there is more than
        // one kind of initalization failure
        if(failure.getTestHeader().contains("initializationError(")){
            currentTest = null;
        } else {
            currentTest.testFailed(failure);
        }

    }

    @Override
    public void testRunFinished(Result result) {
        Log.i("Test run finished", "Finished");
    }

    @Override
    public void testFinished(Description description) {

        Log.i("Test Finished", description.getDisplayName());
        if(currentTest != null) { //current test will be null if it is being silently discarded
            currentTest.calculateExcutionTime(System.currentTimeMillis());
            competedTests.add(currentTest); //add the current test to the list
        }
    }

    @Override
    public void testIgnored(Description description){
        Log.i("Test ignored", description.getDisplayName());
    }

    @Override
    public void testAssumptionFailure(Failure failure){
        Log.i("Test assumption failed", failure.getDescription().getDisplayName());
    }

    @Override
    public void testRunStarted(Description description){
        Log.i("Run started", description.getDisplayName());

    }

    @Override
    public void testStarted(Description description){
        Log.i("Test started", description.getDisplayName());

        //create test result object
        if(description.isTest()) {
            currentTest = new TestResults(description.getDisplayName());
        } else {
            //anything other than test is not currently supported
            //reset current test since we are on an unsupportedTest
            currentTest = null;

        }
    }

    public List<TestResults> getCompletedTests(){
        return competedTests;
    }

}
