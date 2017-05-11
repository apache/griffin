package org.apache.griffin.core.measure;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.List;

/**
 * Created by xiangrchen on 5/10/17.
 */
public class AssertAnnotations {
    private static void assertAnnotations(
            List<Class> annotationClasses, List<Annotation> annotations) {
        // length
        if (annotationClasses.size() != annotations.size()) {
            throw new AssertionError(
                    String.format("Expected %d annotations, but found %d"
                            , annotationClasses.size(), annotations.size()
                    ));
        }
        // exists
        annotationClasses.forEach(
                ac -> {
                    long cnt
                            = annotations.stream()
                            .filter(a -> a.annotationType().isAssignableFrom(ac))
                            .count();
                    if (cnt == 0) {
                        throw new AssertionError(
                                String.format("No annotation of type %s found", ac.getName())
                        );
                    }
                }
        );
    }
    public static void assertType(Class c, Class... annotationClasses) {
        assertAnnotations(
                Arrays.asList(annotationClasses)
                , Arrays.asList(c.getAnnotations())
        );
    }
    public static void assertField(
            Class c, String fieldName, Class... annotationClasses) {
        try {
            assertAnnotations(
                    Arrays.asList(annotationClasses)
                    , Arrays.asList(c.getDeclaredField(fieldName).getAnnotations())
            );
        } catch (NoSuchFieldException nsfe) {
            throw new AssertionError(nsfe);
        }
    }
    public static void assertMethod(
            Class c, String getterName, Class...annotationClasses) {
        try {
            assertAnnotations(
                    Arrays.asList(annotationClasses)
                    , Arrays.asList(c.getDeclaredMethod(getterName).getAnnotations())
            );
        } catch (NoSuchMethodException nsfe) {
            throw new AssertionError(nsfe);
        }
    }
}
