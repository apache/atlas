---
name: Relationship Search
route: /SearchRelationship
menu: Documentation
submenu: Search
---

import  themen  from 'theme/styles/styled-colors';
import  * as theme  from 'react-syntax-highlighter/dist/esm/styles/hljs';
import SyntaxHighlighter from 'react-syntax-highlighter';
import Img from 'theme/components/shared/Img'

# Relationship Search

Apache Atlas is a metadata governance tool that features the ability to search for pre-defined entities of the defined model.

We are able to search for these entities by specifying generic attributes (such as ‘name’, ‘qualified name’, ‘description’ etc), entity type definition specific attributes (attributes unique to certain entities such as ‘cluster name’ for hdfs_path type entities) and also on parameters like classifications applied, sub-types of entities and deleted entities. However these results only return entities which fulfil the parameters of the search queries.

A relationship, which also follows a similar structure to an entity, describes various metadata between two entity end-points. For example, let us assume that in the case of a relationship between hive_table and hive_db, the relationship type name is called hive_table_db and it can have its own metadata which can be added as an attributes to this model.

The Relationship search allows you to get the metadata between two entity model, by querying using relationship type name, and it also has support for filtering on the relationship attribute(s).

The entire query structure can be represented using the following JSON structure (called RelationshipSearchParameters)

<SyntaxHighlighter wrapLines={true} language="json" style={theme.dark}>
{`{
  "relationshipName":       "hive_table_db",
  "excludeDeletedEntities": true,
  "offset":                 0,
  "limit":                  25,
  "relationshipFilters":    {  },
  "sortBy":                 "table_name",
  "sortOrder":              ASCENDING,
  "marker":                 "*"
}`}
</SyntaxHighlighter>

**Field description**

 <SyntaxHighlighter wrapLines={true} language="json" style={theme.dark}>
   {`relationshipName:       the type of relationship to look for
excludeDeletedEntities: should the search exclude deleted entities? (default: true)
offset:                 starting offset of the result set (useful for pagination)
limit:                  max number of results to fetch
relationshipFilters:    relationship attribute filter(s)
sortBy:                 attribute to which results are sorted by
sortOrder:              sorting order of results
marker:                 add either offset or marker, value of marker for first page will be '*'
                        and value of nextMarker: in the response will be input of marker for subsequent pages`}
</SyntaxHighlighter>

Attribute based filtering can be done on multiple attributes with AND/OR conditions.

**Real time example of Relationship Search**

Consider use-case of social media based application '*Instagram*' and its basic functionalities like:

* *User* creates *Posts*
* *User* reacts to *Posts*
* *User* creates *Highlight*
* *Posts* added in *Highlight*

Below is the Metadata Model highlighting above use-case

<Img src={`/images/twiki/relationship_search_model.png`} height="500" width="840"/>

Example:
Consider a user Ajay, who had uploaded his celebratory Diwali photo as a post. Users like Divya, Rahul and many others reacted with a like to his post.

Now the user Ajay wants to find out the number of 'like' reactions to his post.

For his search, he will focus on 'user_post' relationship type and filter those with attributes where reactions have 'like' in them and in which the post name has 'Diwali' in it.

**Example of filtering (for user_post attributes)**

<SyntaxHighlighter wrapLines={true} language="json" style={theme.dark}>
{`   {
     "relationshipName":       "user_post",
     "excludeDeletedEntities": true,
     "offset":                 0,
     "limit":                  25,
     "relationshipFilters": {
        "condition": "AND",
        "criterion": [
           {
              "attributeName":  "reaction",
              "operator":       "eq",
              "attributeValue": "like"
           },
           {
              "attributeName":  "post_name",
              "operator":       "contains",
              "attributeValue": "Diwali"
           }
        ]
     }
   }`}
</SyntaxHighlighter>

**Supported operators for filtering**

Same operators as basic search

**CURL Samples**

<SyntaxHighlighter wrapLines={true} language="shell" style={theme.dark}>
{`curl -sivk -g
    -u <user>:<password>
    -X POST
    -d '{
     "relationshipName":       "user_post",
     "excludeDeletedEntities": true,
     "offset":                 0,
     "limit":                  25,
     "relationshipFilters": {
        "condition": "AND",
        "criterion": [
           {
              "attributeName":  "reaction",
              "operator":       "eq",
              "attributeValue": "like"
           },
           {
              "attributeName":  "post_name",
              "operator":       "contains",
              "attributeValue": "Diwali"
           }
        ]
     }
   }'
    <protocol>://<atlas_host>:<atlas_port>/api/atlas/v2/search/relations`}
</SyntaxHighlighter>
