package cloudant.com.androidtest.keyprovider;

import android.test.AndroidTestCase;

import com.cloudant.sync.datastore.encryption.AndroidKeyProvider;
import com.cloudant.sync.datastore.encryption.EncryptionKey;

import java.util.Arrays;

public class AndroidKeyProviderTest extends AndroidTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testCreateProviderWithIdentifier(){
        AndroidKeyProvider provider1 = new AndroidKeyProvider(getContext(), "provider1password", getProviderId());

        EncryptionKey storeKey = provider1.getEncryptionKey();
        assertNotNull(storeKey);

        EncryptionKey loadKey = provider1.getEncryptionKey();
        assertNotNull(loadKey);

        assertTrue(Arrays.equals(storeKey.getKey(), loadKey.getKey()));
    }

    public void testCreateTwoProvidersWithIdentifier(){
        AndroidKeyProvider provider1 = new AndroidKeyProvider(getContext(), "provider1password", getProviderId());
        AndroidKeyProvider provider2 = new AndroidKeyProvider(getContext(), "provider2password", getProviderId());

        EncryptionKey provider1Key = provider1.getEncryptionKey();
        assertNotNull(provider1Key);

        EncryptionKey provider2Key = provider2.getEncryptionKey();
        assertNotNull(provider2Key);

        assertFalse(Arrays.equals(provider1Key.getKey(), provider2Key.getKey()));
    }

    private String getProviderId(){
        return "providerId-"+System.currentTimeMillis();

    }
}
