[![Build Status](https://travis-ci.org/meltzg/GenMapred.svg?branch=master)](https://travis-ci.org/meltzg/GenMapred)
[![codecov](https://codecov.io/gh/meltzg/GenMapred/branch/master/graph/badge.svg)](https://codecov.io/gh/meltzg/GenMapred)

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
hadoop jar GenMapred.jar org.meltzg.genmapred.runner.GenJobRunner --primary primary.json [--secondary secondary.json] [--httpfs url for HttpFS]
```

The GenJobRunner is a configurable Hadoop MapReduce Job runner.  It can be configured using the values in the primary and, optionally, the secondary JSON files.  Values in the primary, WILL NOT be overridden by values in the secondary.  Properties in the primary whose value has the ```appendable``` sub-property set to "true" will have the secondary's value appended with the GenJobConfiguration.PropValue.VAL_DELIMITER.

### Configuration Schema

```JSON
{
	"propertyName": {
		"val": "property value",
		"isAppendable": false
	}
}
```

### GenJobRunner Standard Configuration Options

Required fields must be present in the primary or secondary configurations

Key | Value | Required | Notes
--- | --- | --- | ---
jobName | name to give job | ```true``` | 
mapperClass | fully qualified mapper class name | ```true``` | 
partitionerClass | fully qualified partitioner class name | ```false``` | 
sortComparatorClass | fully qualified sort comparator class name | ```false``` | 
combinerClass | fully qualified combiner class name | ```false``` | 
combinerComparatorClass | fully qualified combiner key grouping comparator class name | ```false``` | 
groupingComparatorClass | fully qualified grouping comparator class name | ```false``` | 
reducerClass | fully qualified reducer class name | ```true``` | 
inputPath | job input path | ```true``` | 
outputPath | job output path | ```true``` | 
outputKeyClass | fully qualified output key class name | ```true``` | 
outputValueClass | fully qualified output value class name | ```true``` | 
artifactJars | '\|' delimited list of fully qualified jar paths | ```false``` | Is only required if configured classes are not already in the Hadoop Classpath


#### Notes
 - The ```artifactJars``` property must have jar paths separated by '|'
 - The values for class name properties must be the fully qualified class names for the classes you want to use.
 - As of right now, these custom configuration objects are not available to the MapReduce components
 
### Example Donfigurations

#### Primary.json

```JSON
{
	"jobName": {
		"val": "test",
		"isAppendable": false
	},
	"outputValueClass": {
		"val": "org.apache.hadoop.io.IntWritable",
		"isAppendable": false
	},
	"inputPath": {
		"val": "/activity/*/*accelerometer*",
		"isAppendable": false
	},
	"outputPath": {
		"val": "/activity-res",
		"isAppendable": false
	},
	"foo": {
		"val": "foobar",
		"isAppendable": false
	},
	"outputKeyClass": {
		"val": "org.apache.hadoop.io.Text",
		"isAppendable": false
	}
}
```

#### Secondary.json

```JSON
{
	"artifactJars": {
		"val": "/home/hduser/playground/examples.jar",
		"isAppendable": true
	},
	"reducerClass": {
		"val": "org.meltzg.genmapred.examples.ModelCountReducer",
		"isAppendable": false
	},
	"mapperClass": {
		"val": "org.meltzg.genmapred.examples.ModelCountMapper",
		"isAppendable": false
	}
}
```

#### Final configuration
The primary and secondary configurations will be merged into the following JSON object

```JSON
{
	"jobName": {
		"val": "test",
		"isAppendable": false
	},
	"outputValueClass": {
		"val": "org.apache.hadoop.io.IntWritable",
		"isAppendable": false
	},
	"inputPath": {
		"val": "/activity/*/*accelerometer*",
		"isAppendable": false
	},
	"outputPath": {
		"val": "/activity-res",
		"isAppendable": false
	},
	"foo": {
		"val": "foobar",
		"isAppendable": false
	},
	"outputKeyClass": {
		"val": "org.apache.hadoop.io.Text",
		"isAppendable": false
	},
	"artifactJars": {
		"val": "/home/hduser/playground/examples.jar",
		"isAppendable": true
	},
	"reducerClass": {
		"val": "org.meltzg.genmapred.examples.ModelCountReducer",
		"isAppendable": false
	},
	"mapperClass": {
		"val": "org.meltzg.genmapred.examples.ModelCountMapper",
		"isAppendable": false
	}
}
```
