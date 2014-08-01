package com.cloudant.imageshare;

import android.os.AsyncTask;
import android.util.Log;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import java.net.URI;

/**
 * Queries the authentication server to create a new database.
 */
public class AsyncRequestNewDB extends AsyncTask<String, Void, String> {

    @Override
    protected String doInBackground(String... params) {
        try {
            URI uri = new URI(params[0]);
            HttpClient httpClient = new DefaultHttpClient();
            HttpGet get = new HttpGet(uri);
            HttpResponse response = httpClient.execute(get);
            StatusLine l = response.getStatusLine();
            if (l.getStatusCode() != 200) {
                Log.d("Status: ", l.getReasonPhrase());
                return null;
            }
            String resp_string = EntityUtils.toString(response.getEntity());
            return resp_string;
        } catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
}
