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
package com.facebook.presto.error;

import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.testng.Assert.fail;

public class TestErrorKeys
{
    @Test
    public void testIfAllErrorKeysAreDefined()
            throws IOException
    {
        Properties errorMessages = new Properties();
        errorMessages.load(this.getClass().getClassLoader().getResourceAsStream("error/Messages.properties"));
        List<ErrorKeys> errorKeysWithoutValues = new ArrayList<>();
        for (ErrorKeys errorKey : ErrorKeys.values()) {
            if (!errorMessages.containsKey(errorKey.name())) {
                errorKeysWithoutValues.add(errorKey);
            }
        }
        if (errorKeysWithoutValues.size() > 0) {
            fail("Messages.properties does not contain values for " + errorKeysWithoutValues);
        }
    }
}
