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

package com.cloudant.mazha;

import com.cloudant.mazha.json.JSONHelper;
import com.cloudant.sync.util.TestUtils;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;

public class AnimalDb {

    public static void populateWithoutFilter(CouchClient client) {
        try {
            JSONHelper jsonHelper =  new JSONHelper();
            for(int i = 0 ; i < 10 ; i ++) {
                String animal = FileUtils.readFileToString(TestUtils.loadFixture("fixture/animaldb/animaldb_animal" + i + ".json"));
                Response response = client.create(jsonHelper.fromJson(new StringReader(animal)));
                Assert.assertTrue(response.getOk());
            }
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    public static void populate(CouchClient client) {
        try {
            JSONHelper jsonHelper =  new JSONHelper();
            for(int i = 0 ; i < 10 ; i ++) {
                String animal = FileUtils.readFileToString(TestUtils.loadFixture("fixture/animaldb" +
                        "/animaldb_animal" + i + ".json"));
                Response response = client.create(jsonHelper.fromJson(new StringReader(animal)));
                Assert.assertTrue(response.getOk());
            }
            String filter = FileUtils.readFileToString(TestUtils.loadFixture("fixture/animaldb/animaldb_filter.json"));
            Response response = client.create(jsonHelper.fromJson(new StringReader(filter)));
            Assert.assertTrue(response.getOk());
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }
}
