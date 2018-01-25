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

The GenJobRunner is a configurable Hadoop MapReduce Job runner.  It can be configured using the values in the primary and, optionally, the secondary xml files.  Values in the primary, WILL NOT be overridden by values in the secondary.  ```customConfs``` maps will be merged, but not checked for uniqueness.

### Configuration Schema

The schema definition can be found [here](./src/main/resources/GenJobConfiguration.xsd).

#### Notes
 - The ```artifactJar``` elements must contain the fully quallified path of the jar files
 - The keys for the ```customConfs``` ```entry``` must be the fully qualified class names for the custom configuration objects you define.
 - As of right now, these custom configuration objects are not available to the MapReduce components
 - Each ```customConfs``` ```entry``` can have a sequence of the same object type