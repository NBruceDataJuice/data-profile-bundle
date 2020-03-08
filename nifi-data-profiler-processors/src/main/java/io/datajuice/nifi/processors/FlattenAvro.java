package io.datajuice.nifi.processors;

import io.datajuice.nifi.processors.utils.Convert;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;

import java.beans.PropertyDescriptor;
import java.util.*;

import static io.datajuice.nifi.processors.Relationships.FAILURE;
import static io.datajuice.nifi.processors.Relationships.SUCCESS;

public class FlattenAvro extends AbstractProcessor {

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
            session.write(incomingAvro, Convert::convertAvroFile);
        } catch (Exception e){
            session.transfer(incomingAvro, FAILURE);
            return;
        }

        session.transfer(incomingAvro, SUCCESS);
    }

}
