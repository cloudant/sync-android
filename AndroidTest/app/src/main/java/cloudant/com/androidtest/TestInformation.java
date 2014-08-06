package cloudant.com.androidtest;

import android.app.Activity;
import android.content.Intent;
import android.os.Bundle;
import android.view.Menu;
import android.view.MenuItem;
import android.widget.TextView;


public class TestInformation extends Activity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_test_information);

        Intent intent = this.getIntent();

        TextView testName = (TextView)findViewById(R.id.testname);
        testName.setText(intent.getStringExtra(BundleConstants.TEST_NAME));

        TextView reason = (TextView)findViewById(R.id.failureReason);
        reason.setText(intent.getStringExtra(BundleConstants.FAILURE_REASON));

        TextView exceptionStack = (TextView)findViewById(R.id.exception);
        exceptionStack.setText(intent.getStringExtra(BundleConstants.EXCEPTION_STACK));

    }


    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.test_information, menu);
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

}
