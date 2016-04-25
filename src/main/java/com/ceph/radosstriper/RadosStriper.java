package com.ceph.radosstriper;

import com.ceph.rados.RadosBase;

/**
 * Created by arno.broekhof on 4/25/16.
 */
public class RadosStriper extends RadosBase {

    private static String ENV_BUFFER_SIZE = System.getenv("RADOS_JAVA_BUFFER_SIZE");
    private static String ENV_STRIPE_UNIT = System.getenv("RADOS_JAVA_STRIP_UNIT");
    private static String ENV_STRIPE_COUNT = System.getenv("RADOS_JAVA_STRIPE_COUNT");
    private static String ENV_OBJECT_SIZE = System.getenv("RADOS_JAVA_OBJECT_SIZE");


    // BUFFER_SIZE defaults to 8M
    private static final String BUFFER_SIZE = ENV_BUFFER_SIZE == null ? "8388608" : ENV_BUFFER_SIZE;

    // STRIPE_UNIT defaults to 512k
    private static final String STRIPE_UNIT = ENV_STRIPE_UNIT == null ? "524288" : ENV_STRIPE_UNIT;

    // OBJECT_SIZE defaults to 4M
    private static final String OBJECT_SIZE = ENV_OBJECT_SIZE == null ? "4194304" : ENV_OBJECT_SIZE;

    // STRIPE_COUNT defaults to 3
    private static final String STRIPE_COUNT = ENV_STRIPE_COUNT == null ? "3" : ENV_STRIPE_COUNT;


}
