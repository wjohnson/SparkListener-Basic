package listener;

import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SQLExecution;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyListener extends org.apache.spark.scheduler.SparkListener {

  private static final Logger log = LoggerFactory.getLogger("MyLogger");

  public MyListener() {
    log.info("INITIALIZING");
  }

  @Override
  public void onOtherEvent(SparkListenerEvent event) {
    log.info("MYLISTENER: ON OTHER EVENT END");
    log.info(String.format("ON OTHER EVENT of TYPE: %s",event.getClass().getName()));

  }

  @Override
  public void onJobEnd(SparkListenerJobEnd jobEnd) {
    log.info("MYLISTENER: ON JOB END");

  }

  @Override
  public void onJobStart(SparkListenerJobStart jobStart) {
    log.info("MYLISTENER: ON JOB START");
    Set<String> keys = jobStart.properties().stringPropertyNames();
    for (String key : keys){
      log.info(String.format("%s: %s", key, jobStart.properties().getProperty(key)));
    }
    // Check if there is an execution id
    if (jobStart.properties().containsKey("spark.sql.execution.id")){
      long executionId = Long.parseLong(jobStart.properties().getProperty("spark.sql.execution.id"));
      QueryExecution queryExecution = SQLExecution.getQueryExecution(executionId);
      LogicalPlan lp = queryExecution.optimizedPlan();
      log.info(String.format("Working with a lp in %s",lp.getClass().getName()));
      log.info(lp.prettyJson());

      getAbfsPath(lp);

    }else{
      log.error("FAILED TO FIND spark.sql.execution.id for job id", jobStart.jobId());
    }
  }

  @Override
  public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
    log.info("MYLISTENER: ON APP START");
  }


  private void getAbfsPath(LogicalPlan lp){

    if (lp instanceof LogicalRelation){
      log.info("LP IS a LogicalRelation");
    }

    if ((lp instanceof LogicalRelation) && ((LogicalRelation) lp).relation() instanceof HadoopFsRelation){
      LogicalRelation logRel = (LogicalRelation) lp;
      HadoopFsRelation relation = (HadoopFsRelation) logRel.relation();

      log.info("LOGGING INPUT FILES");
      for (String s : relation.inputFiles()){
        log.info(s);
      }
      

    }else{
      log.info("THE LP WAS NOT a Logical Relation or HadoopFsRelation");
    }
  //   return context
  //       .getSparkSession()
  //       .map(
  //           session -> {
  //             Configuration hadoopConfig =
  //                 session.sessionState().newHadoopConfWithOptions(relation.options());
  //             return JavaConversions.asJavaCollection(relation.location().rootPaths()).stream()
  //                 .map(p -> PlanUtils.getDirectoryPath(p, hadoopConfig))
    

  //   Path p;
  //   Configuration hadoopConf;
  //   try {
  //     if (p.getFileSystem(hadoopConf).getFileStatus(p).isFile()) {
  //       return p.getParent();
  //     } else {
  //       return p;
  //     }
  //   } catch (IOException e) {
  //     log.warn("Unable to get file system for path ", e);
  //     return p;
  //   }
  // }
  }

}
