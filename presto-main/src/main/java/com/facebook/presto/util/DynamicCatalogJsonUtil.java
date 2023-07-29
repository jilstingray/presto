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

import com.facebook.airlift.log.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;

public class DynamicCatalogJsonUtil
{
    private static final Logger log = Logger.get(DynamicCatalogJsonUtil.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private DynamicCatalogJsonUtil() {}

    public static <T> T toObject(String json, Class<T> clazz)
    {
        try {
            return mapper.readValue(json, clazz);
        }
        catch (Exception e) {
            log.error("Failed to convert json to object", e);
            return null;
        }
    }

    public static <T> List<T> toObjectArray(String json, Class<T> clazz)
    {
        try {
            return mapper.readValue(json, mapper.getTypeFactory().constructCollectionType(List.class, clazz));
        }
        catch (Exception e) {
            log.error("Failed to convert json to object array", e);
            return null;
        }
    }

    public static <T> String toJson(T object)
    {
        try {
            return mapper.writeValueAsString(object);
        }
        catch (Exception e) {
            log.error("Failed to convert object to json", e);
            return null;
        }
    }
}
