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

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 * Created by rhys on 01/09/2014.
 */
public class TestResultStorage  {

    private static TestResultStorage instance;
    private List<TestResults> testResults;

    static {
        instance = new TestResultStorage();
    }

    private TestResultStorage(){
            testResults = new LinkedList<TestResults>();
    }

    public static TestResultStorage getInstance(){
        return instance;
    }


    public void add(TestResults t){
        testResults.add(t);
    }

    public int size(){
        return testResults.size();
    }

    public TestResults get(int i){
        return testResults.get(i);
    }

    public void deleteAll(){
        testResults = new LinkedList<TestResults>();
    }

    public List<TestResults> getAll(){
        return testResults;
    }

}
