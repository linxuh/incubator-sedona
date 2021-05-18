<img src="./sedona_logo.png" width="400">

[![Scala and Java build](https://github.com/apache/incubator-sedona/workflows/Scala%20and%20Java%20build/badge.svg)](https://github.com/apache/incubator-sedona/actions?query=workflow%3A%22Scala+and+Java+build%22) [![Python build](https://github.com/apache/incubator-sedona/workflows/Python%20build/badge.svg)](https://github.com/apache/incubator-sedona/actions?query=workflow%3A%22Python+build%22) ![Example project build](https://github.com/apache/incubator-sedona/workflows/Example%20project%20build/badge.svg)

Click [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/apache/incubator-sedona/HEAD?filepath=binder) and play the interactive Sedona Python Jupyter Notebook immediately!


Apache Sedona™(incubating) is a cluster computing system for processing large-scale spatial data. Sedona extends Apache Spark / SparkSQL with a set of out-of-the-box Spatial Resilient Distributed Datasets (SRDDs)/ SpatialSQL that efficiently load, process, and analyze large-scale spatial data across machines.

### Sedona contains several modules:

| Name  |  API |  Introduction|
|---|---|---|
|Core  | RDD  | SpatialRDDs and Query Operators. |
|SQL  | SQL/DataFrame  |SQL interfaces for Sedona core.|
|Viz |  RDD, SQL/DataFrame | Visualization for Spatial RDD and DataFrame|
|Zeppelin |  Apache Zeppelin | Plugin for Apache Zeppelin 0.8.1+|

### AdaptiveGrid branch
this branch add adaptiveGrid representation of spatial object, supporting an adaptive grid partitioning method and spatial index.In addition,spatial join using adptivegrid is achieve, which perform better than origin method 

### Sedona supports several programming languages: Scala, Java, SQL, Python and R.

## Compile the source code

Please refer to [Sedona website](http://sedona.apache.org/download/compile/)

## Contact

Twitter: [Sedona@Twitter](https://twitter.com/ApacheSedona)

Gitter chat: [![Gitter](https://badges.gitter.im/apache/sedona.svg)](https://gitter.im/apache/sedona?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

[Sedona JIRA](https://issues.apache.org/jira/projects/SEDONA): Bugs, Pull Requests, and other similar issues

[Sedona Mailing Lists](https://lists.apache.org/list.html?sedona.apache.org): 

* [dev@sedona.apache.org](https://lists.apache.org/list.html?dev@sedona.apache.org): project development, general questions or tutorials

# Please visit [Apache Sedona website](http://sedona.apache.org/) for detailed information

## Powered by

<img src="./incubator_logo.png" width="400">
