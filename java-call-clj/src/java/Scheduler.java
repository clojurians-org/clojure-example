import java.util.Map ;
import java.util.HashMap ;
import clojure.lang.IFn ;
import clojure.java.api.Clojure ;

public class Scheduler {
  static {
    IFn require = Clojure.var("clojure.core", "require") ;
    require.invoke(Clojure.read("cheshire.core")) ;
  }

  public static Scheduler parse(bytes[] zdata) {
    IFn parse_string = Clojure.var("cheshire.core", "parse-string") ;
    return new Scheduler((Map)parse_string.invoke(new String(zdata))) ;
  }

  public Map state ;
  public void init(Map state) {
    this.state = state ;
  }
  public Scheduler(Map state) {
    init(state) ;
  }

  public Integer getMaxRunningTasks() {
    return (Integer)this.state.get("max-running-tasks") ;
  }
  public Integer getMaxAccumCost() {
    return (Integer)this.state.get("max-accum-cost") ;
  }
  public Integer getMaxSingleCost() {
    return (Integer)this.state.get("max-single-cost") ;
  }
}
