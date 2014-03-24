package com.cloudant.mazha;

import com.cloudant.mazha.json.JSONHelper;
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
                String animal = FileUtils.readFileToString(new File("fixture/animaldb/animaldb_animal" + i + ".json"));
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
                String animal = FileUtils.readFileToString(new File("fixture/animaldb/animaldb_animal" + i + ".json"));
                Response response = client.create(jsonHelper.fromJson(new StringReader(animal)));
                Assert.assertTrue(response.getOk());
            }
            String filter = FileUtils.readFileToString(new File("fixture/animaldb/animaldb_filter.json"));
            Response response = client.create(jsonHelper.fromJson(new StringReader(filter)));
            Assert.assertTrue(response.getOk());
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }
}
