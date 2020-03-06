package io.datajuice.nifi.processors.data.profiler;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;

import java.beans.PropertyDescriptor;
import java.util.*;

import static io.datajuice.nifi.processors.data.profiler.Relationships.FAILURE;
import static io.datajuice.nifi.processors.data.profiler.Relationships.SUCCESS;
import static io.datajuice.nifi.processors.data.profiler.Util.iterateDatatypeMap;

public class DataProfiler extends AbstractProcessor {
    // TODO analysis to understand what properties would be useful
    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    public void init(final ProcessorInitializationContext context){
        List<PropertyDescriptor> properties = new ArrayList<>();
        this.properties = Collections.unmodifiableList(properties);

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public void onTrigger(ProcessContext context, final ProcessSession session)
            throws ProcessException {


        FlowFile incomingAvro = session.get();
        if (incomingAvro == null) {
            return;
        }

        // TODO add ability to write bad records to a separate file
        try{
            iterateDatatypeMap(session, incomingAvro);
        } catch (Exception e){
            session.transfer(incomingAvro, FAILURE);
            return;
        }

        session.transfer(incomingAvro, SUCCESS);
    }
}
