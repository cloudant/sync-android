/*
 *   Copyright (c) 2016 IBM Corporation. All rights reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 *   except in compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software distributed under the
 *   License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *   either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */

package com.cloudant.sync.query;

import com.cloudant.sync.datastore.Datastore;
import com.cloudant.sync.datastore.QueryableDatastore;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by rhys on 07/07/2016.
 */
public class QueryInvocationHandler implements InvocationHandler {


    protected final Datastore datastore;
    public QueryInvocationHandler(Datastore ds) {
        datastore = ds;
    }

    // This is used the get the query executor from subclasses, the datastore passed in the proxy
    // datastore, this makes sure all calls to the underlying datastore go through the proxy.
    public QueryExecutor createQueryExecutor(Datastore proxy) {
        return new QueryExecutor(proxy, ((QueryableDatastore)this.datastore).getQueryQueue());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.getName().equals("find")){
            Map<String, Object> indexes = datastore.listIndexes();

            // Create the Args list
            List<Object> argsList = new ArrayList<Object>(Arrays.asList(args));

            int argCount = argsList.size();


            // this works by knowing the defaults for the full arg method, this will also work if
            // we introduce more overrides.
            // each case is supposed to drop down to the next.
            switch(argCount){
                case 1:
                    //skip
                    argsList.add(0L);
                case 2:
                    // limit
                    argsList.add(0L);
                case 3:
                    //fields
                    argsList.add(null);
                case 4:
                    //sortDoc
                    argsList.add(null);
                default:
                    break;
            }

            args = argsList.toArray();
            assert args.length == 5;


            return createQueryExecutor((Datastore) proxy).find((Map<String, Object>) args[0],
                    indexes,
                    (Long) args[1],
                    (Long) args[2],
                    (List<String>)args[3],
                    (List<Map<String,String>>)args[4]);
        } else {
            return method.invoke(datastore, args);
        }
    }

    public static class SQL extends QueryInvocationHandler {

        public SQL(Datastore ds){
            super(ds);
        }

        @Override
        public QueryExecutor createQueryExecutor(Datastore proxy) {
            return new MockSQLOnlyQueryExecutor(proxy, ((QueryableDatastore)datastore).getQueryQueue());
        }
    }

    public static class Matcher extends QueryInvocationHandler {

        public Matcher(Datastore ds) {
            super(ds);
        }

        @Override
        public QueryExecutor createQueryExecutor(Datastore proxy) {
            return new MockMatcherQueryExecutor(proxy, ((QueryableDatastore)datastore).getQueryQueue());
        }
    }
}
