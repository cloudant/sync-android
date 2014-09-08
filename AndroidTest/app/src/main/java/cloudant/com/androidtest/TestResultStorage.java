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
