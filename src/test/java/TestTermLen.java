import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import models.CompositeKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class TestTermLen {

    byte[] term = new byte[65535 + 1];

    @Before
    public void setUp() {
        for (int i = 0; i < 65535 + 1; i++) {
            term[i] = 65; // Add an "A"
        }
    }

    @Test
    public void testTerm() throws IOException {
        CompositeKey key = new CompositeKey(new String(term));
        key.write(new DataOutputStream(new FileOutputStream("file")));
        // Will fail if CompositeKey.getTerm() is return this.term;
    }
}
