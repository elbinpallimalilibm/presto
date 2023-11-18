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

import com.facebook.presto.spi.error.BaseErrorKey;

public enum DeltaErrorKey implements BaseErrorKey
{
    DELTA_NOT_SUPPORTED_NOT_THE_EXPECTED_TABLE_NAME_FORMAT,
    DELTA_UNSUPPORTED_COLUMN_TYPE_UNSUPPORTED_DATATYPE_FOR_PARTITION_COLUMN,
    DELTA_INVALID_PARTITION_VALUE_CANNOT_PARSE_PARTITION_VALUE,
    DELTA_UNSUPPORTED_COLUMN_TYPE_COLUMN_CONTAINS_UNSUPPORTED_DATATYPE,
    DELTA_INVALID_PARTITION_VALUE_CANNOT_PARSE_PARTITION_VALUE_IN_FILE,
    DELTA_UNSUPPORTED_DATA_FORMAT_HAS_UNSUPPORTED_DATA_FORMAT,
    DELTA_MISSING_DATA_ERROR_OPENING_HIVE_SPLIT,
    DELTA_CANNOT_OPEN_SPLIT_OPENING_HIVE_SPLIT,
    DELTA_PARQUET_SCHEMA_MISMATCH_COLUMN_TYPE_DECLARED_MISMATCH
}
