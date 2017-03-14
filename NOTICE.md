# Stroom Stats

Copyright 2016 Crown Copyright

This software project uses libraries which fall under several licences. The purpose of this `NOTICE.md` file is to contain notices that are required by copyright owners and their licences. The full texts of all the licences used by third party libraries are included in the `licences` folder.

See the file [LICENCE.txt](./LICENCE.txt) for licensing information for this project.

The design of fundamental parts of Stroom Stats (particularly the table design, row key structure and unique ID concept) were heavily based on that of [OpenTSDB](https://github.com/OpenTSDB/opentsdb). Stroom Stats uses a modified version of OpenTSDB's UniqueId class. It is for these reasons that Stroom Stats is released under the LGPL licence to match OpenTSDB.

The table below includes licences for all Maven dependencies. 

Group                     | Artifact             | Version             | Licence
--------------------------|----------------------|---------------------|--------------
org.slf4j                 | slf4j-api            | 6.0-beta.1-SNAPSHOT | MIT
org.slf4j                 | log4j-over-slf4j     | 6.0-beta.1-SNAPSHOT | MIT
org.slf4j                 | jcl-over-slf4j       | 6.0-beta.1-SNAPSHOT | MIT
org.assertj               | assertj-core         | 3.6.2               | Apache 2.0
org.apache.curator        | curator-framework    | 2.11.1              | Apache 2.0
org.apache.curator        | curator-recipes      | 2.11.1              | Apache 2.0
org.apache.curator        | curator-x-discovery  | 2.11.1              | Apache 2.0
net.bytebuddy             | byte-buddy           | 1.5.8               | Apache 2.0
commons-lang              | commons-lang         | 2.6                 | Apache 2.0
org.apache.curator        | curator-recipes      | 2.11.1              | Apache 2.0
org.apache.curator        | curator-test         | 2.11.1              | Apache 2.0
io.dropwizard             | dropwizard-core      | 1.0.5               | Apache 2.0
io.dropwizard             | dropwizard-hibernate | 1.0.5               | Apache 2.0
io.dropwizard             | dropwizard-testing   | 1.0.5               | Apache 2.0
io.dropwizard             | dropwizard-client    | 1.0.5               | Apache 2.0
com.github.toastshaman    | dropwizard-auth-jwt  | 1.0.2-0             | Apache 2.0
de.spinscale.dropwizard   | dropwizard-jobs-core | 2.0.1               | Apache 2.0
org.ehcache               | ehcache              | 3.2.0               | Apache 2.0
com.google.guava          | guava                | 14.0                | Apache 2.0
com.google.inject         | guice                | 4.0                 | Apache 2.0
org.apache.hbase          | hbase-shaded-client  | 1.2.0               | Apache 2.0
io.javaslang              | javaslang            | 2.0.5               | Apache 2.0
joda-time                 | joda-time            | 2.9.4               | Apache 2.0
junit                     | junit                | 4.12                | EPL 1.0
org.apache.kafka          | kafka-clients        | 0.10.1.0            | Apache 2.0
org.apache.kafka          | kafka-streams        | 0.10.1.0            | Apache 2.0
com.esotericsoftware      | kryo-shaded          | 4.0.0               | Custom BSD
ch.qos.logback            | logback-classic      | 1.1.8               | EPL 1.0/LGPL 2.1
ch.qos.logback            | logback-core         | 1.1.8               | EPL 1.0/LGPL 2.1
org.mockito               | mockito-core         | 2.+                 | MIT
mysql                     | mysql-connector-java | 5.1.40              | GPL (Exceptions apply)
org.reflections           | reflections          | 0.9.10              |
org.springframework.kafka | spring-kafka-test    | 1.1.3.RELEASE       |
com.beust                 | jcommander           | 1.48                |

\* The mysql-client-connector is distributed under a GPL license but has exceptions that allow it to be distributed with OSS without requiring the GPL license to be applied to the OSS. See [http://www.mysql.com/about/legal/licensing/foss-exception/](http://www.mysql.com/about/legal/licensing/foss-exception/) for details.

