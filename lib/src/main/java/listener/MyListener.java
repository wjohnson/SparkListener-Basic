package listener;

import java.util.Set;

import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
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
    log.info("ON OTHER EVENT of TYPE:",event.getClass().getName());

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
      log.info(key, jobStart.properties().getProperty(key));
    }
  }

  @Override
  public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
    log.info("MYLISTENER: ON APP START");
  }

}
