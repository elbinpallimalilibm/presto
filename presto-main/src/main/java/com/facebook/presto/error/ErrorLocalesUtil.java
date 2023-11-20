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

import com.facebook.airlift.log.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ErrorLocalesUtil
{
    private static Pattern messagePropertiesPattern = Pattern.compile("Messages_([a-z]+)_([A-Z]+)\\.properties");
    private static Logger log = Logger.get(ErrorLocalesUtil.class);

    private ErrorLocalesUtil()
    {}

    public static List<Locale> getAvailableErrorMessageLocales()
    {
        List<Locale> availableLocales = new ArrayList<>();

        File resourcesFolder = Paths.get("etc/resources/error/").toFile();
        addLocalesFromFolderToList(resourcesFolder, availableLocales);
        return availableLocales;
    }

    public static List<Locale> getAvailableErrorMessageLocalesForPlugin(ClassLoader pluginClassLoader)
    {
        List<Locale> availableLocales = new ArrayList<>();
        try {
            Enumeration<URL> errorFolderURLs = pluginClassLoader.getResources("error");
            List<URL> errorFolderURLList = Collections.list(errorFolderURLs);
            for (URL errorFolderURL : errorFolderURLList) {
                try {
                    // We cannot create a file from a jar resource and hence
                    // cannot list files inside a jar folder.
                    if (!errorFolderURL.toURI().getScheme().equals("jar")) {
                        File errorFolder = Paths.get(errorFolderURL.toURI()).toFile();
                        addLocalesFromFolderToList(errorFolder, availableLocales);
                    }
                }
                catch (URISyntaxException e) {
                    log.error(e, "Error getting handle to error folder");
                }
            }
        }
        catch (IOException e) {
            log.error(e, "Error loading resources");
        }

        return availableLocales;
    }

    private static void addLocalesFromFolderToList(File errorFolder, List<Locale> localesList)
    {
        if (errorFolder.exists() && errorFolder.isDirectory()) {
            File[] messagesFiles = errorFolder.listFiles((dir, name) -> {
                if (messagePropertiesPattern.matcher(name).matches()) {
                    return true;
                }
                else {
                    return false;
                }
            });

            for (File messageFile : messagesFiles) {
                Matcher localeMatcher = messagePropertiesPattern.matcher(messageFile.getName());
                if (localeMatcher.matches()) {
                    String language = localeMatcher.group(1);
                    String region = localeMatcher.group(2);
                    localesList.add(new Locale.Builder().setLanguage(language).setRegion(region).build());
                }
            }
        }
    }
}
