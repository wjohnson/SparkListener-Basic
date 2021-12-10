package listener;

import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.atomic.AtomicInteger;


public class MyListener extends org.apache.spark.scheduler.SparkListener {

  private static final Logger log = LoggerFactory.getLogger("MyLogger");
  private AtomicInteger jobcount = new AtomicInteger(0);

  @Override
  public void onOtherEvent(SparkListenerEvent event) {
    log.info("ONOTHER");
    
  }

  @Override
  public void onJobStart(SparkListenerJobStart jobStart) {
    log.info("ON JOB START");
  
  }

  @Override
  public void onJobEnd(SparkListenerJobEnd jobEnd) {
    log.info("ON JOB END");
    jobcount.incrementAndGet();
    log.info(String.format("Jobs so far: %s", jobcount.toString()));

  }
  
  @Override
  public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
    log.info("ON APP START");
  }

}
