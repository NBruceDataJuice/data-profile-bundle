# data-profile-bundle
A NiFi bundle of processors to allow data profiling while in transit

The current build is based off of the utility package from Apache Gobblin and the Kite processors that used to be distributed with NiFi. 

## As-Is
Processor runs tests on utility functions and a Flatten Avro processor itself. Processor Manifest does not include the processor to put into flow. Once tested, within a flow this will be updated. 

## TODO 
Run tests stated above. Improve the code and add more functionality to the Flatten Avro processor

Once the Flatten Avro processor is in place. I'll start to consume the open source project [Data Cleaner](https://datacleaner.github.io/) to determine if any artifacts from that library will be useful here. 

## How is this Flatten Avro processor different than the Kite processor?
The processor provided by Kite needed a few manual steps. Some of the properties could be automated to allow the schemas to be set dynamically. However, ironically enough, I haven't figured out a way to programmatically set the dynamic mapping properties needed for the processor to correctly write to the flattened schema. 
