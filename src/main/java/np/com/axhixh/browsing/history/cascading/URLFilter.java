package np.com.axhixh.browsing.history.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.TupleEntry;

/**
 *
 * @author ashish
 */
public class URLFilter extends BaseOperation<Object> implements Filter<Object>{
    
    @Override
    public boolean isRemove(FlowProcess fp, FilterCall<Object> fc) {
        TupleEntry arguments = fc.getArguments();
        String url = arguments.getString("url");
        return !url.toLowerCase().contains("clojure");
    }
}
