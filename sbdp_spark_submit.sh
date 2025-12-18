spark-submit --master yarn --deploy-mode cluster \
--py-files sbdp_lib.zip --files conf/sbdp.cnf,conf/spark.conf,conf/log4j2.xml \
sbdp_main.py qa 2022-08-02