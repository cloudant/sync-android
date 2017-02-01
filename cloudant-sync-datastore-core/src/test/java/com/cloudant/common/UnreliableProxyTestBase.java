/*
 * Copyright © 2015, 2017 IBM Corp. All rights reserved.
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

import com.cloudant.sync.internal.replication.ReplicationTestBase;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.Toxic;
import eu.rekawek.toxiproxy.model.ToxicDirection;

import java.util.UUID;

/**
 * Created by tomblench on 23/07/15.
 */
public class UnreliableProxyTestBase extends ReplicationTestBase {

    // Proxy configuration
    private final static String PROXY_HOST = System.getProperty("test.couch.proxy.host",
            "localhost");
    // this is the toxiproxy admin port for setting up proxies and toxics
    private final static int PROXY_ADMIN_PORT = Integer.parseInt(System.getProperty("test.couch" +
                    ".proxy.admin.port",
            "8474"));
    // this is where the toxiproxy will listen (note uses test.couch.port so the tests will use the
    // proxy not the real couch port).
    private final static int PROXY_PORT = Integer.parseInt(System.getProperty("test.couch.port",
            "8000"));

    // Couch configuration
    private final static String COUCH_HOST = System.getProperty("test.couch.host", "localhost");
    // this is where couchdb listens
    private final static int COUCH_TARGET_PORT = Integer.parseInt(System.getProperty("test.couch" +
                    ".proxy.target.port",
            "5984"));

    // Configuration for the timout toxic
    /**
     * Specifiy the fraction of connections that will be impacted by the toxic.
     */
    private static final float TOXICITY = 0.5f;
    /**
     * Specify the time in ms before the connection breaks.
     */
    private static final long TIMEOUT = 50l;
    /**
     * Name for the timeout toxic, based on the timeout and toxicity parameters.
     */
    private static final String TIMEOUT_NAME = "Timeout_" + TIMEOUT + "_" + TOXICITY;

    // A toxiproxy client to administer the toxiproxy config
    private static ToxiproxyClient toxicClient = new ToxiproxyClient(PROXY_HOST, PROXY_ADMIN_PORT);

    // A toxiproxy instance for running unreliability tests
    private static Proxy toxiProxy = null;

    /**
     * Set up a proxy with a unique name for this test run, all requests will be routed through the
     * proxy, but it will only be toxic after some toxics are added to it by the tests.
     *
     * @throws Exception
     */
    @BeforeClass
    public static void addProxy() throws Exception {
        // For now we proxy all tests in the same way
        toxiProxy = toxicClient.createProxy(UUID.randomUUID().toString(), PROXY_HOST + ":" +
                PROXY_PORT, COUCH_HOST + ":" + COUCH_TARGET_PORT);
    }

    /**
     * Remove any added toxics after each test
     *
     * @throws Exception
     */
    @After
    public void removeToxics() throws Exception {
        for (Toxic t : toxiProxy.toxics().getAll()) {
            t.remove();
        }
    }

    /**
     * Delete the proxy created by this test run
     *
     * @throws Exception
     */
    @AfterClass
    public static void deleteProxy() throws Exception {
        if (toxiProxy != null) {
            toxiProxy.delete();
        }
    }

    /**
     * Add a default timeout toxic
     *
     * @throws Exception
     */
    protected void addTimeoutToxic() throws Exception {
        toxiProxy.toxics().timeout(TIMEOUT_NAME, ToxicDirection.DOWNSTREAM, TIMEOUT).setToxicity
                (TOXICITY);
    }

    protected void removeTimeoutToxic() throws Exception {
        Toxic t = toxiProxy.toxics().get(TIMEOUT_NAME);
        if (t != null) {
            t.remove();
        }
    }
}
