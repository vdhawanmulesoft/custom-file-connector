# Custom File Connector

This is a custom connector which  iterates the file line by line and groups the lines of code starting with VI and FC. Once the group size is 10000 VI records the mule message is sent to the flow. At the end file is deleted.


...


...


Add this dependency to your application pom.xml

```
<groupId>orgId</groupId>
<artifactId>custom-file-connector</artifactId>
<version>1.0.0</version>
<classifier>mule-plugin</classifier>
```
