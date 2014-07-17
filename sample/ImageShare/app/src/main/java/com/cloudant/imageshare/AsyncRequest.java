package com.cloudant.imageshare;

import android.os.AsyncTask;
import android.util.Log;
import android.util.Pair;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by pettyurin on 7/17/14.
 */
public class AsyncRequest extends AsyncTask<String, String, Pair>{

    @Override
    protected Pair doInBackground(String... params) {
        try {
            URI uri = new URI(params[0]);
            HttpClient httpClient = new DefaultHttpClient();
            HttpPut put = new HttpPut(uri);
            List<NameValuePair> pairs = new ArrayList<NameValuePair>();
            pairs.add(new BasicNameValuePair("link", params[1]));
            put.setEntity(new UrlEncodedFormEntity(pairs));
            HttpResponse response = httpClient.execute(put);
            String resp_string = EntityUtils.toString(response.getEntity());
            JSONObject json = new JSONObject(resp_string);
            String key = json.get("key").toString();
            String pass = json.get("password").toString();
            Pair pair = new Pair(key,pass);
            return pair;
        } catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    @Override
    protected void onPostExecute(Pair result){
        Log.d("KEY", (String) result.first);
        Log.d("Pass", (String) result.second);
    }
}
