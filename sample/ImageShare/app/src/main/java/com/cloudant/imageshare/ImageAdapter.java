package com.cloudant.imageshare;

import android.content.Context;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import android.widget.GridView;
import android.widget.ImageView;

import java.util.ArrayList;

public class ImageAdapter extends BaseAdapter{

    private Context mContext;
    //layout size
    private int lSize;

    public ImageAdapter(Context c, int layoutParam) {
        mContext = c;
        lSize = layoutParam;
        mThumbIds = new ArrayList<Bitmap>();
        Bitmap img0 = BitmapFactory.decodeResource(c.getResources(), R.drawable.sample_0);
        Bitmap img1 = BitmapFactory.decodeResource(c.getResources(), R.drawable.sample_1);
        mThumbIds.add(img0);
        mThumbIds.add(img1);
    }

    public int getCount() {
        return mThumbIds.size();
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

        imageView.setImageBitmap(mThumbIds.get(position));
        return imageView;
    }

    public void addImage(Bitmap img){
        mThumbIds.add(img);
    }

    // references to our images
    private ArrayList<Bitmap> mThumbIds;
            /*R.drawable.sample_2, R.drawable.sample_3
            R.drawable.sample_4, R.drawable.sample_5,
            R.drawable.sample_6, R.drawable.sample_7,
            R.drawable.sample_0, R.drawable.sample_1,
            R.drawable.sample_2, R.drawable.sample_3,
            R.drawable.sample_4, R.drawable.sample_5,
            R.drawable.sample_6, R.drawable.sample_7,
            R.drawable.sample_0, R.drawable.sample_1,
            R.drawable.sample_2, R.drawable.sample_3,
            R.drawable.sample_4, R.drawable.sample_5,
            R.drawable.sample_6, R.drawable.sample_7*/
}
