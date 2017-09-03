# Simple Storage Service (S3) API for Scala
This repository provides API libraries for accessing Simple Storage Service (S3). 

# Installation Requirements
JDK 1.8

Maven 3.x

# Installation
In order to install the library, execute as follows:

```
git clone git@github.com:cafebazaar/s3-api-scala.git
cd s3-api-scala
mvn install
```
# Usage
Maven:
```
<dependency>
    <groupId>ir.cafebazaar.kandoo</groupId>
    <artifactId>s3api</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```
SBT:
```
libraryDependencies += "ir.cafebazaar.kandoo" % "s3api" % "0.1.0-SNAPSHOT" % "provided"
```
Gradle:
```
provided group: 'ir.cafebazaar.kandoo', name: 's3api', version: '0.1.0-SNAPSHOT'
```
