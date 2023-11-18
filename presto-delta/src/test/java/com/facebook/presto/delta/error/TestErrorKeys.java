/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.delta.error;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

public class TestErrorKeys
{
    @Test
    public void testIfAllErrorKeysAreDefined()
    {
        ResourceBundle bundle = ResourceBundle.getBundle("error/Messages", Locale.US);
        assertNotNull(bundle, "Unable to get Messages resource bundle");
        List<DeltaErrorKey> errorKeysWithoutValues = new ArrayList<>();
        for (DeltaErrorKey errorKey : DeltaErrorKey.values()) {
            if (!bundle.containsKey(errorKey.name())) {
                errorKeysWithoutValues.add(errorKey);
            }
        }
        if (errorKeysWithoutValues.size() > 0) {
            fail("Messages resource bundle does not contain values for " + errorKeysWithoutValues);
        }
    }
}
