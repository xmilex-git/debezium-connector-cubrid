/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cubrid.jna;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.ptr.LongByReference;
import com.sun.jna.ptr.PointerByReference;

/**
 * JNA binding for the CUBRID CDC log-extraction C API (cubrid_log.h), exported by
 * {@code libcubridcs.so} in {@code $CUBRID/lib}. JNA dlopen()s the library itself, so the
 * Kafka Connect plugin classloader rules for JNI do not apply (workspace#32); locate the
 * .so via {@code jna.library.path} or {@code LD_LIBRARY_PATH} — it must NOT live under
 * plugin.path.
 *
 * <p>Linux x86_64 only for the POC: {@code time_t} and {@code uint64_t} are both mapped
 * as Java {@code long}.
 */
public interface CubridLogLibrary extends Library {

    String LIBRARY_NAME = "cubridcs";

    CubridLogLibrary INSTANCE = Native.load(LIBRARY_NAME, CubridLogLibrary.class);

    /* return codes (cubrid_log.h) */
    int CUBRID_LOG_EXTRACTION_TIMEOUT = -6;
    int CUBRID_LOG_SUCCESS = 0;
    int CUBRID_LOG_SUCCESS_WITH_NO_LOGITEM = 1;
    int CUBRID_LOG_SUCCESS_WITH_ADJUSTED_LSA = 2;

    /* configuration step */
    int cubrid_log_set_connection_timeout(int timeout);

    int cubrid_log_set_extraction_timeout(int timeout);

    int cubrid_log_set_tracelog(String path, int level, int filesize);

    int cubrid_log_set_max_log_item(int maxLogItem);

    int cubrid_log_set_all_in_cond(int retrieveAll);

    int cubrid_log_set_extraction_table(long[] classoidArr, int arrSize);

    int cubrid_log_set_extraction_user(String[] userArr, int arrSize);

    /* preparation step */
    int cubrid_log_connect_server(String host, int port, String dbname, String id, String password);

    /** in/out: timestamp (time_t*), out: flat LSA (uint64_t*, low48=pageid / high16=offset). */
    int cubrid_log_find_lsa(LongByReference timestamp, LongByReference lsa);

    /* extraction step */
    int cubrid_log_extract(LongByReference lsa, PointerByReference logItemList, IntByReference listSize);

    int cubrid_log_clear_log_item(Pointer logItemList);

    /* finalization step */
    int cubrid_log_finalize();
}
