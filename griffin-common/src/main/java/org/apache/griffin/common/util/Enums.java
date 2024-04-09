package org.apache.griffin.common.util;

import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;


public class Enums {

    public static <E extends Enum<E>> Map<String, E> toMap(Class<E> enumType) {
        return toMap(enumType, Enum::name);
    }
    public static <K, E extends Enum<E>> Map<K, E> toMap(Class<E> enumType, Function<E, K> keyMapper) {
        return Arrays.stream(enumType.getEnumConstants())
                .collect(ImmutableMap.toImmutableMap(keyMapper, Function.identity()));
    }

    public static <E extends Enum<E>> void checkDuplicated(Class<E> enumType, Function<E, ?> mapper) {
        E[] values = enumType.getEnumConstants();
        for (int n = values.length, i = 0; i < n; i++) {
            for (int j = i + 1; j < n; j++) {
                Object v1 = mapper.apply(values[i]), v2 = mapper.apply(values[j]);
                if (Objects.equals(v1, v2)) {
                    throw new Error(enumType.getSimpleName() + " enums duplicated enum error code: " + v1);
                }
            }
        }
    }

}

