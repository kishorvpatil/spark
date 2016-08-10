package org.apache.spark.launcher;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * Created by kpatil on 8/9/16.
 */
class SparkSubmitRunner implements Runnable {
  private Method main;
  private final List<String> args;

  SparkSubmitRunner(Method main, List<String> args) {
    this.main = main;
    this.args = args;
  }

  SparkSubmitRunner(List<String> args) {
    this.args = args;
  }

  protected static Method getSparkSubmitMain() throws ClassNotFoundException, NoSuchMethodException {
      Class<?> cls = Class.forName("org.apache.spark.deploy.SparkSubmit");
      return cls.getDeclaredMethod("main", String[].class);
  }

  @Override
  public void run() {
    try {
      if(main == null) {
        main = getSparkSubmitMain();
      }
      Object argsObj = args.toArray(new String[args.size()]);
      main.invoke(null, argsObj);
    } catch (IllegalAccessException illAcEx) {
      throw new RuntimeException(illAcEx);
    } catch (InvocationTargetException invokEx) {
      invokEx.printStackTrace();
      throw new RuntimeException(invokEx);
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
