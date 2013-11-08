package np.com.axhixh.browsing.history.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import java.util.Calendar;

/**
 *
 * @author ashish
 */
public class TimeSplit extends BaseOperation<Object> implements Function<Object>{

    public static final Fields OUTPUT = new Fields("dow", "hr");

    public TimeSplit() {
        super(2, OUTPUT);
    }
    @Override
    public void operate(FlowProcess fp, FunctionCall<Object> fc) {
        TupleEntry arguments = fc.getArguments();
        Long visitDateInNano = arguments.getLong("visit_date");

        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(visitDateInNano/1000);

        int dow = cal.get(Calendar.DAY_OF_WEEK);
        int hr = cal.get(Calendar.HOUR_OF_DAY);

        Tuple result = new Tuple(dow, hr);
        fc.getOutputCollector().add(result);
    }

}