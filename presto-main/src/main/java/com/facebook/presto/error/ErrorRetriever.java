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

import java.net.URLClassLoader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

import static java.util.Objects.requireNonNull;

public class ErrorRetriever
{
    private static boolean isErrorI18nEnabled;
    private static List<Locale> localesToLoad = Collections.emptyList();
    private static Map<Locale, CombinedResourceBundle> resourceBundles = new HashMap<>();
    private static final Logger log = Logger.get(ErrorRetriever.class);
    private static final Locale defaultLocale = Locale.US;

    private ErrorRetriever() {}

    public static void initialiseErrorBundles(boolean isErrorI18nEnabled)
    {
        ErrorRetriever.isErrorI18nEnabled = isErrorI18nEnabled;

        // Add default resource bundle
        CombinedResourceBundle defaultResourceBundle = new CombinedResourceBundle(defaultLocale);
        resourceBundles.put(Locale.US, defaultResourceBundle);
        ResourceBundle bundle = ResourceBundle.getBundle("error/Messages", defaultLocale);
        defaultResourceBundle.addToResources(bundle);

        if (isErrorI18nEnabled) {
            // If internationalization is enabled, create resource bundles
            // for each of the locale to be loaded.
            for (Locale locale : localesToLoad) {
                CombinedResourceBundle resourceBundleForLocale = new CombinedResourceBundle(locale);
                resourceBundles.put(locale, resourceBundleForLocale);
                bundle = ResourceBundle.getBundle("error/Messages", locale);
                resourceBundleForLocale.addToResources(bundle);
            }
        }
    }

    public static void addErrorMessagesFromPlugin(URLClassLoader pluginClassLoader, String plugin)
    {
        try {
            ResourceBundle bundle = ResourceBundle.getBundle("error/Messages", defaultLocale, pluginClassLoader);
            resourceBundles.get(defaultLocale).addToResources(bundle);
        }
        catch (MissingResourceException e) {
            log.debug("No bundle available for error/Messages in plugin %s", plugin);
        }

        if (isErrorI18nEnabled) {
            for (Locale locale : localesToLoad) {
                try {
                    ResourceBundle bundle = ResourceBundle.getBundle("error/Messages", locale, pluginClassLoader);
                    resourceBundles.get(locale).addToResources(bundle);
                }
                catch (MissingResourceException e) {
                    log.debug("No bundle available for error/Messages in plugin %s for locale %s", plugin, locale);
                }
            }
        }
    }

    public static String getErrorMessage(String errorKey, Locale locale)
    {
        if (isErrorI18nEnabled) {
            requireNonNull(locale, "locale cannot be null");
        }
        else {
            locale = defaultLocale;
        }

        ResourceBundle selectedBundle = resourceBundles.get(locale);
        if (selectedBundle == null) {
            // If we are trying to load a resource we don't have,
            // load the default resource instead.
            selectedBundle = resourceBundles.get(defaultLocale);
        }
        return selectedBundle.getString(errorKey);
    }
}
