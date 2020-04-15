package com.atguigu.ct.common.api;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;

/**
 * 列族列名对应映射
 */
@Target({FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {
    String family() default "info";
    String column() default "";
}
