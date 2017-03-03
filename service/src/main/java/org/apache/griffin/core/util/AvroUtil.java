package org.apache.griffin.core.util;

import org.apache.avro.Schema;

public class AvroUtil {

    public static Schema schemaOf(String schema) {
        return new Schema.Parser().parse(schema);
    }

}
