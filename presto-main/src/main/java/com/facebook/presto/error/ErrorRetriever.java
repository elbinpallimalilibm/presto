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
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.Optional;
import java.util.ResourceBundle;

import static java.util.Objects.requireNonNull;

public class ErrorRetriever
{
    private static boolean isErrorI18nEnabled;
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
            List<Locale> localesToLoad = ErrorLocalesUtil.getAvailableErrorMessageLocales();
            log.info("Locales to load %s", localesToLoad);

            for (Locale locale : localesToLoad) {
                CombinedResourceBundle resourceBundleForLocale = new CombinedResourceBundle(locale);
                resourceBundles.put(locale, resourceBundleForLocale);
            }
            File resourcesFolder = Paths.get("etc/resources").toFile();
            if (resourcesFolder.exists() && resourcesFolder.isDirectory()) {
                try {
                    URLClassLoader resourcesClassLoader = new URLClassLoader(
                            new URL[] {(resourcesFolder.toURI().toURL())});
                    for (Locale locale : localesToLoad) {
                        bundle = ResourceBundle.getBundle("error/Messages", locale, resourcesClassLoader);
                        resourceBundles.get(locale).addToResources(bundle);
                    }
                }
                catch (MalformedURLException e) {
                    log.error(e, "Error loading locale specific Messages.properties");
                }
            }
        }
    }

    public static void addErrorMessagesFromPlugin(URLClassLoader pluginClassLoader, String plugin)
    {
        Optional<ResourceBundle> defaultBundle = Optional.empty();
        try {
            defaultBundle = Optional.of(ResourceBundle.getBundle("error/Messages", defaultLocale, pluginClassLoader));
            resourceBundles.get(defaultLocale).addToResources(defaultBundle.get());
        }
        catch (MissingResourceException e) {
            log.debug("No bundle available for error/Messages in plugin %s", plugin);
        }

        // We need atleast the default Messages bundle before we try loading the
        // localized bundles
        if (defaultBundle.isPresent() && isErrorI18nEnabled) {
            List<Locale> localesToLoad = ErrorLocalesUtil.getAvailableErrorMessageLocalesForPlugin(pluginClassLoader);
            log.info("Plugin %s locales to load %s", plugin, localesToLoad);
            for (Locale locale : localesToLoad) {
                try {
                    ResourceBundle bundle = ResourceBundle.getBundle("error/Messages", locale, pluginClassLoader);
                    // If the locale we are trying to load was not present in presto-main
                    // we will create a CombinedResourceBundle for this locale by copying
                    // the default locale.
                    if (!resourceBundles.containsKey(locale)) {
                        resourceBundles.put(locale, resourceBundles.get(defaultLocale));
                    }
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
        String errorMessage;
        try {
            errorMessage = selectedBundle.getString(errorKey);
        }
        catch (MissingResourceException e) {
            // If the property value was not found in locale specific resource
            // try loading from default property file.
            errorMessage = resourceBundles.get(defaultLocale).getString(errorKey);
        }

        return errorMessage;
    }
}
