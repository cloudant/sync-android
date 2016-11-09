/*
 * Copyright (c) 2015 IBM Corp. All rights reserved.
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

package com.cloudant.android;

import com.cloudant.sync.internal.android.Base64OutputStreamFactory;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

/**
 * Created by Rhys Short on 12/06/15.
 */
public class Base64OutputStreamFactoryTest extends Object {

    private final String password="k8b:2KcU2re:BSTeyYg:4hUsu+zWgg#85+!22VUuf:22@2ESzQKZwwv+:/S3Qd79";

    @Test
    public void testInputStreamContainsNoNewLines() throws Exception {

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        OutputStream outputStream = Base64OutputStreamFactory.get(byteArrayOutputStream);
        outputStream.write(password.getBytes());
        Assert.assertFalse(byteArrayOutputStream.toString().contains("\n"));
    }
}