package com.cloudant.imageshare;

import android.content.Context;
import android.content.CursorLoader;
import android.database.Cursor;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.net.Uri;
import android.provider.MediaStore;
import android.util.Log;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import android.widget.GridView;
import android.widget.ImageView;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

public class ImageAdapter extends BaseAdapter{

    private Context mContext;
    //layout size
    private int lSize;

    public ImageAdapter(Context c, int layoutParam) {
        mContext = c;
        lSize = layoutParam;
        mThumbBitmaps = new ArrayList<Bitmap>();
        mThumbFiles = new ArrayList<InputStream>();
        /*Bitmap img0 = BitmapFactory.decodeResource(c.getResources(), R.drawable.sample_0);
        Bitmap img1 = BitmapFactory.decodeResource(c.getResources(), R.drawable.sample_1);
        mThumbBitmaps.add(img0);
        mThumbBitmaps.add(img1);*/
    }

    public int getCount() {
        return mThumbBitmaps.size();
    }

    public Object getItem(int position) {
        return null;
    }

    public long getItemId(int position) {
        return 0;
    }

    // create a new ImageView for each item referenced by the Adapter
    public View getView(int position, View convertView, ViewGroup parent) {
        ImageView imageView;
        if (convertView == null) {  // if it's not recycled, initialize some attributes
            imageView = new ImageView(mContext);
            imageView.setLayoutParams(new GridView.LayoutParams(lSize, lSize));
            imageView.setScaleType(ImageView.ScaleType.CENTER_CROP);
            imageView.setPadding(8, 8, 8, 8);
        } else {
            imageView = (ImageView) convertView;
        }

        imageView.setImageBitmap(mThumbBitmaps.get(position));
        return imageView;
    }

    public void addImage(Uri imageUri, Context c) throws IOException{
        mThumbFiles.add(c.getContentResolver().openInputStream(imageUri));
        //mThumbFiles.add(c.openFileInput(imageUri.);
        Bitmap bitmap = MediaStore.Images.Media.getBitmap(c.getContentResolver(), imageUri);
        mThumbBitmaps.add(bitmap);
    }

    public void loadImage(InputStream is, Context c){
        mThumbFiles.add(is);
        Bitmap bitmap = BitmapFactory.decodeStream(is);
        mThumbBitmaps.add(bitmap);
    }

    public InputStream getStream(int position) throws FileNotFoundException{
        return mThumbFiles.get(position);
    }

    public void clearImageData(){
        mThumbBitmaps.clear();
        mThumbFiles.clear();
    }

    // references to our images
    private ArrayList<Bitmap> mThumbBitmaps;
    private ArrayList<InputStream> mThumbFiles;
}
