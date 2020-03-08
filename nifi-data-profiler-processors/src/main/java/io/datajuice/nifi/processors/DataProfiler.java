package io.datajuice.nifi.processors;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;

import java.beans.PropertyDescriptor;
import java.util.*;

import static io.datajuice.nifi.processors.Relationships.*;
import static io.datajuice.nifi.processors.utils.ProfileManager.profileManager;

public class DataProfiler extends AbstractProcessor {
    // TODO analysis to understand what properties would be useful Initial thoughts
    //  1) A config that tells exactly what type each column is. Could work off of something like filename,
    //      to allow for multiple mappings to live in single file OR make a user route on that attribute before hand
    //  2) Config that tells each profiler what columns to profile
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


        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        // TODO add ability to write bad records to a separate file
        try{
            session.transfer(profileManager(session, flowFile), SUCCESS);
            session.remove(flowFile);
        } catch (Exception e){
            session.transfer(flowFile, FAILURE);
        }
    }
}