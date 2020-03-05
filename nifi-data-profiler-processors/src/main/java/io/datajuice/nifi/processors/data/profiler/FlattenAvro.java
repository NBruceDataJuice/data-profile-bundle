package io.datajuice.nifi.processors.data.profiler;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;

import java.beans.PropertyDescriptor;
import java.util.*;

import static io.datajuice.nifi.processors.data.profiler.Relationships.SUCCESS;

public class FlattenAvro extends AbstractProcessor {

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


    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(ProcessContext context, final ProcessSession session)
            throws ProcessException {


        FlowFile incomingAvro = session.get();
        if (incomingAvro == null) {
            return;
        }

        try{
            session.write(incomingAvro, Util::convertAvroFile);
        } catch (Exception e){

        }

        session.transfer(incomingAvro, SUCCESS);
    }

}
