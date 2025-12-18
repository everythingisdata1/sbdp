spark-submit --master yarn --deploy-mode cluster \
--py-files sbdp_lib.zip --files conf/sbdp.cnf,conf/spark.conf,conf/log4j2.xml \
--driver-cors 2\
--driver-memory 10G\
--conf spark.driver.memoryoverhead 1G\
sbdp_main.py qa 2022-08-02