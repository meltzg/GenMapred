# GenMapred
A generic and configurable job runner for Hadoop Mapreduce

## Requirements

 - Hadoop +2.X
 
## Build

```
gradle clean build
```

## Usage

```
hadoop jar GenMapred.jar org.meltzg.genmapred.runner.GenJobRunner primary.xml [secondary.xml]
```

The GenJobRunner is a configurable Hadoop MapReduce Job runner.  It can be configured using the values in the primary and, optionally, the secondary xml files.  Values in the primary, WILL NOT be overridden by values in the secondary.  Properties in the primary whose value has the ```appendable``` attribute set to "true" will have the secondary's value appended with the GenJobConfiguration.PropValue.VAL_DELIMITER.

### Configuration Schema

The schema definition can be found [here](./src/main/resources/GenJobConfiguration.xsd).

### GenJobRunner Standard Configuration Options

Required fields must be present in the primary or secondary configurations

Key | Value | Required | Notes
--- | --- | --- | ---
jobName | name to give job | ```true``` | 
mapperClass | fully qualified mapper class name | ```true``` | 
combinerClass | fully qualified combiner class name | ```false``` | 
reducerClass | fully qualified reducer class name | ```true``` | 
inputPath | job input path | ```true``` | 
outputPath | job output path | ```true``` | 
outputKeyClass | fully qualified output key class name | ```true``` | 
outputValueClass | fully qualified output value class name | ```true``` | 
artifactJars | '|' delimited list of fully qualified jar paths | ```false``` | Is only required if configured classes are not already in the Hadoop Classpath


#### Notes
 - The ```artifactJars``` property must have jar paths separated by '|'
 - The values for class name properties must be the fully qualified class names for the classes you want to use.
 - As of right now, these custom configuration objects are not available to the MapReduce components
