package org.apache.atlas.annotation;

import org.apache.atlas.utils.OnAtlasEnableCondition;
import org.springframework.context.annotation.Conditional;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Conditional(OnAtlasEnableCondition.class)
public @interface EnableConditional {
    // Configured atlas property
    String property();
    // The default interface implementation should declare this as true
    boolean isDefault() default false;
}