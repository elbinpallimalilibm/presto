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
package com.facebook.presto.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ErrorSerDeUtils
{
    private ErrorSerDeUtils()
    {}

    public static List<List<Byte>> convertObjectArrayToBytes(Object[] objects)
            throws IOException
    {
        List<List<Byte>> listOfLists = new ArrayList<>();
        for (Object o : objects) {
            listOfLists.add(serializeData(getSanitizedObject(o)));
        }

        return listOfLists;
    }

    public static Object[] convertBytesToObjectArray(List<List<Byte>> listOfArgs)
            throws IOException, ClassNotFoundException
    {
        Object[] objects = new Object[listOfArgs.size()];
        int i = 0;
        for (List<Byte> arg : listOfArgs) {
            objects[i++] = deserializeData(arg);
        }

        return objects;
    }

    public static List<Byte> serializeData(Object data)
            throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream out = new ObjectOutputStream(bos);
            out.writeObject(data);
            out.flush();
            byte[] byteArray = bos.toByteArray();
            return Arrays.asList(getBoxedArray(byteArray));
        }
        finally {
            try {
                bos.close();
            }
            catch (IOException ex) {
                // ignore close exception
            }
        }
    }

    private static Object getSanitizedObject(Object data)
    {
        if ((data instanceof Boolean) ||
                (data instanceof Byte) ||
                (data instanceof Number) ||
                (data instanceof String)) {
            return data;
        }
        else {
            return data.toString();
        }
    }

    public static Object deserializeData(List<Byte> byteList)
            throws IOException, ClassNotFoundException
    {
        byte[] byteArray = getUnboxedArray(byteList.toArray(new Byte[] {}));
        ByteArrayInputStream bis = new ByteArrayInputStream(byteArray);
        ObjectInputStream in = null;
        try {
            in = new ObjectInputStream(bis);
            Object o = in.readObject();
            return o;
        }
        finally {
            try {
                if (in != null) {
                    in.close();
                }
            }
            catch (IOException ex) {
                // ignore close exception
            }
        }
    }

    private static Byte[] getBoxedArray(byte[] bytes)
    {
        Byte[] boxedArray = new Byte[bytes.length];
        int i = 0;
        for (byte b : bytes) {
            boxedArray[i++] = b;
        }

        return boxedArray;
    }

    private static byte[] getUnboxedArray(Byte[] bytes)
    {
        byte[] unboxedArray = new byte[bytes.length];
        int i = 0;
        for (Byte b : bytes) {
            unboxedArray[i++] = b;
        }

        return unboxedArray;
    }
}
