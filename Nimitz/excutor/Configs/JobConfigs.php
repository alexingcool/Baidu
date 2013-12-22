<?php
/**
 * @file JobConfigs.php
 * @author zhangliang(com@baidu.com)
 * date 2013/12/12 16:36:51
 * @brief
 *
 **/
 
 class JobConfigs {
 	const ZKADDRESS = "szwg-qatest-dpf005.szwg01.baidu.com:2181";
 	const ZKPATH = "/storage/dtmeta";
 	const BIGTABLENAME = "udw_event";
 	const USER = "InternalUser";
 	const IFILE = "{WORK_PATH}/input.schema.out";
 	const OFILE = "{WORK_PATH}/output.schema.out";
 	const COMPRESS = "true";
 	const CODEC = "org.apache.hadoop.io.compress.LzoCodec";
 	const SEPARATOR = "0x0";
 	const LIBAPIJAR = "{LIB_JARS_PATH}/../lib.new/udw-program-api.jar";
 	const LIBTHRIFTJAR = "{LIB_JARS_PATH}/../lib.new/libthrift-0.8.0.jar";
 	const LIBSERDEJAR = "{LIB_JARS_PATH}/../lib.new/hive-serde-0.7.1.plus.jar";
 	const LIBCOMMONJAR = "{LIB_JARS_PATH}/../lib.new/hive-common-2.2.0.jar";
 	const LIBHIVEQLJAR = "{LIB_JARS_PATH}/../lib.new/hive-ql-2.2.0.jar";
 	const LIBGSONJAR = "{LIB_JARS_PATH}/../lib.new/gson-udw.jar";
 	const LIBPROTOJAR = "{LIB_JARS_PATH}/../lib.new/protobuf-java-2.4.1.jar";
 	const ETLFRAMEWORKTAR = "etl_framwork.tar";
 	const DATAQTAR = "data_quality.tar";
 	const MAPPER = "ExecMapper_DtMeta";
 	const REDUCER = "ExecReducer_DtMeta";
 	const RUNSHPATH = "/app/ns/udw/etl/etl_framework/dtmeta_event/bin/run.sh";
 	const INPUTPATH = "/app/ns/udw/release/etl/etl_framework_temp/input";
 	const OUTPUTPATH = "/app/ns/udw/release/etl/etl_framework_temp/output";
 	
 }