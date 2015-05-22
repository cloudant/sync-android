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

import android.app.ListActivity;
import android.content.Intent;
import android.os.Bundle;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.ArrayAdapter;
import android.widget.ListView;

import net.sqlcipher.database.SQLiteDatabase;

import org.junit.experimental.categories.Categories;
import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.notification.Failure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.transform.TransformerException;

public class MyActivity extends ListActivity {

    private ArrayAdapter<TestResults> mAdapter = null;

    private boolean testsRan = false;
    private TestResultStorage trs;

    private MyActivity ctx = this;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        //Stop the tests being re-run when we are returned to this activity
        if(savedInstanceState == null) {

            setTestProperties();

            //If sqlcipher testing parameter is 'true', load required library for SQLCipher datastore testing
            boolean isSQLCipherEnabled = Boolean.valueOf(System.getProperty("test.sqlcipher.passphrase"));
            if (isSQLCipherEnabled) {
                //Initialize the native libraries for SQLCipher
                SQLiteDatabase.loadLibs(this);
            }

            trs = TestResultStorage.getInstance();
            mAdapter = new ArrayAdapter<TestResults>(this, R.layout.list_view_text);

            if (savedInstanceState != null || trs.size() > 0) {
                testsRan = true;
                mAdapter.addAll(trs.getAll());
            }

            this.setListAdapter(mAdapter);

            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    CloudantListener listener = new CloudantListener();

                    JUnitCore core = new JUnitCore();
                    core.addListener(listener);

                    ArrayList<Class> classes = new ArrayList<Class>();
                    for (Class c : BuildConfig.classToTests) {
                        //Check parameter for specifying which test case package to run
                        String testsToRun = String.valueOf(System.getProperty("test.run-packages"));
                        //Run all classes if JVM parameter for specifying packages does not exist
                        if (!testsToRun.equals("null")) {
                            //If 'common' value is passed as a parameter, run test cases under package com.cloudant
                            if (testsToRun.equals("common") && c.getPackage().toString().contains("common")) {
                                classes.add(c);
                            } else if (testsToRun.equals("mazha") && c.getPackage().toString().contains("mazha")) {
                                classes.add(c);
                            } else if (testsToRun.equals("datastore") && c.getPackage().toString().contains("datastore")) {
                                classes.add(c);
                            } else if (testsToRun.equals("indexing") && c.getPackage().toString().contains("indexing")) {
                                classes.add(c);
                            } else if (testsToRun.equals("query") && c.getPackage().toString().contains("query")) {
                                classes.add(c);
                            } else if (testsToRun.equals("replication") && c.getPackage().toString().contains("replication")) {
                                classes.add(c);
                            } else if (testsToRun.equals("sqlite") && c.getPackage().toString().contains("sqlite")) {
                                classes.add(c);
                            } else if (testsToRun.equals("util") && c.getPackage().toString().contains("util")) {
                                classes.add(c);
                            }
                        } else {
                            classes.add(c);
                        }
                    }


                    // start with an empty filter and then add in our categories to be excluded
                    Filter filter = new Categories.CategoryFilter(null, null);
                    for (Class c : BuildConfig.testExcludes) {
                        filter = filter.intersect(new Categories.CategoryFilter(null, c));
                    }

                    // set up a request with our filter and our classes and run it
                    Request request = Request.classes(classes.toArray(new Class[classes.size()])).filterWith(filter);
                    final Result r = core.run(request);
                    testsRan = true;
                    final List<Failure> failures = r.getFailures();
                    runOnUiThread(new Runnable() {
                        @Override
                        public void run() {
                            if (failures.size() == 0) {
                                ArrayAdapter<String> adapter = new ArrayAdapter<String>(ctx, R.layout.list_view_text);
                                ctx.setListAdapter(adapter);
                                adapter.add("No Failures");
                                adapter.add("Ran " + r.getRunCount() + " tests");
                            } else {
                                for (Failure f : failures) {
                                    if (f.getTestHeader().contains("initializationError("))
                                        continue;
                                    TestResults tr = new TestResults(f);
                                    trs.add(tr);
                                    mAdapter.add(tr);
                                }
                            }
                        }
                    });

                    JUnitXMLFormatter xmlFormatter = new JUnitXMLFormatter();
                    try {
                        xmlFormatter.outputResults(listener.getCompletedTests());
                    } catch (IOException e) {
                        e.printStackTrace(); //TODO do something here
                    } catch (TransformerException e) {
                        e.printStackTrace(); //TODO do something here
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });

            if (!testsRan) {
                thread.start();
            }
        }
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.my, menu);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        // Handle action bar item clicks here. The action bar will
        // automatically handle clicks on the Home/Up button, so long
        // as you specify a parent activity in AndroidManifest.xml.
        int id = item.getItemId();
        if (id == R.id.action_settings) {
            return true;
        }
        return super.onOptionsItemSelected(item);
    }

    @Override
    protected void onListItemClick(ListView l, View v, int position, long id) {
        // get the test failure and launch a new screen for it to display full information about it
        TestResults tr = mAdapter.getItem(position);
         //need to pack and launch new activity

        Intent intent = new Intent();
        intent.setClass(getBaseContext(), TestInformation.class);
        intent.putExtra(BundleConstants.TEST_NAME,tr.testName());
        intent.putExtra(BundleConstants.FAILURE_REASON,tr.failureMessage());
        intent.putExtra(BundleConstants.EXCEPTION_STACK,tr.exceptionStack());

        this.startActivity(intent); // start display

    }

    private void setTestProperties(){
        //set up dexcache this is a workaround for https://code.google.com/p/dexmaker/issues/detail?id=2
        System.setProperty( "dexmaker.dexcache", this.getCacheDir().getPath() );

        for(String[] testOption : BuildConfig.TEST_CONFIG){
                System.setProperty(testOption[0], testOption[1]);
        }

    }
}
