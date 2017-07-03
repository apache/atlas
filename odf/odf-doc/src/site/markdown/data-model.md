<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~     http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->

# Data Model

This section describes the basic data model of how results of discovery services are
stored and how new discovery services can extend and enrich this model.

See the section [ODF Metadata API](odf-metadata-api.html) for general information
on how to retrieve metadata.

You can find the current Atlas data model in the file

	odf-core/src/main/resources/org/apache/atlas/odf/core/metadata/internal/atlas/atlas-odf-model.json

which contains JSON that can be POSTed to the Atlas `types` REST resource to create those types.

## Annotations

All discovery services results are called "annotations". An annotation is an object
that annotates another object with a certain piece of information.
For instance, you could have a `DataClassAnnotation` which has a reference attribute `classifiedObject` linking it to
a column `NAME` of a table `CUSTREC`, a list of reference attributes `classifyingObjects` linking it to business terms.
An additional attribute `confidences` might provide a list of numeric confidence values indicating the "strength" of the
relationship between the classifiedObject and the respective classifyingObject (business term).
 For column `NAME` the list of classifying objects may have a single entry `Customer Name` the list of confidence values
 may have a single value `0.7`.
This annotation expresses the fact that the term classification services registered and active in ODF have come up with a
70% confidence of CUSTREC.NAME representing a customer name.

Technically, an annotation is a subtype of one of the three *base types* which are subtyes of the (abstract)
Atlas type `Annotation`:

- `ProfilingAnnotation`
- `ClassificationAnnotation`
- `RelationshipAnnotation`


A `ProfilingAnnotation` assigns non-reference attributes to an object. It has the following non-reference attributes:

- `annotationType`: The type of annotation. A Json string of the form
   `{"stdType": "DataQualityAnnotation",`
   ` "runtime": "JRE",`
   ` "spec" : “org.apache.atlas.myservice.MyAnnotation"`
   `}`
   where `stdType` is a base or standardized type name (see below for standardized types), `runtime` names the runtime,
   and `spec` is a runtime-specific string which helps the runtime to deal with instances of this type. In case of a Java
   runtime the `spec` is the name of the implementing class which may be a subclass of the `stdType`.
- `analysisRun`: A string that is set to the request Id of the analysis that created it.
(Compare the swagger documentation of the REST resource `analyses`, e.g. [here](https://sdp1.rtp.raleigh.ibm.com:58081/odf-web-0.1.0-SNAPSHOT/swagger/#/analyses).
*Internal Note*: will be replaced with RID
- `summary`: A human-readable string that presents a short summary of the annotation.
Might be used in generic UIs for displaying unknown annotations.
*Internal note*: deprecated
- `jsonProperties`: A string attributes where you can store arbitrary JSON as a string.
Can be used to 'extend' standard annotations.

...and a single referencing attribute:

- `profiledObject`: The object that is annotated by this annotation. In the example above,
this would point to the Column object.


A `ClassificationAnnotation` assigns any number (including 0) of meta data objects to an object.
It has the same non-reference attributes as `ProfilingAnnotation` plus the following reference attributes:

- `classifiedObject`: The object that is annotated by this annotation.
- `classifyingObjects`: List of references to meta data objects classifying the classifiedObject.

A `RelationshipAnnotation`  expresses a relationship between meta data objects.
It has the same non-reference attributes as `ProfilingAnnotation` plus a single reference attribute:

- `relatedObjects`: List of references to related meta data objects.


Note that annotations are implemented as proper Atlas object types and not traits (labels) for these reasons:

- Annotations of the same type but of different discovery service should be able co-exist, for instance,
to be able to compare results of different services downstream.
This is only partly possible with traits.
- Relationships between objects can not easily be modeled with traits.

A discovery service can deliver its results in a base, standardized, or *custom annotation type*. Depending on the type of the
underlying relationship a custom annotation type might be a subtype of `ProfilingAnnotation` (asymmetric, single reference attribute),
`ClassificationAnnotation` (asymmetric, any number of reference attributes), or `RelationshipAnnotation` (symmetric, any number
of reference attributes). A custom annotation type can have additional non-reference attributes that are stored in its `jsonProperties`.

When implemented in Java, the class defining a custom annotation has private fields and corresponding getter/setter methods
representing the additional information.


##Example


For instance, creating a new annotation of type `org.apache.atlas.oli.MyAnnotation` could look like this.

	public class MyAnnotation extends ClassificationAnnotation {
	   String myNewAttribute;

	   public String getMyNewAttribute() {
	      return myNewAttribute;
	   }

	   public void setMyNewAttribute(String myNewAttribute) {
	      this.myNewAttribute = myNewAttribute;
	   }
	}

Annotations can be mapped into standardized meta data objects by a *propagator* which implements the `AnnotationPropagator` interface.
