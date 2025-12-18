sc = spark.sparkContext

jvm = sc._jvm


class SparkJobListener(jvm.org.apache.spark.scheduler.SparkListener):

    def onJobStart(self, jobStart):
        print(f"[JOB STARTED] Job ID: {jobStart.jobId()}")

    def onJobEnd(self, jobEnd):
        print(f"[JOB ENDED] Job ID: {jobEnd.jobId()}, Result: {jobEnd.jobResult()}")

    def onStageSubmitted(self, stageSubmitted):
        stage = stageSubmitted.stageInfo()
        print(f"[STAGE SUBMITTED] Stage ID: {stage.stageId()}, Name: {stage.name()}")

    def onStageCompleted(self, stageCompleted):
        stage = stageCompleted.stageInfo()
        print(f"[STAGE COMPLETED] Stage ID: {stage.stageId()}, Name: {stage.name()}")
