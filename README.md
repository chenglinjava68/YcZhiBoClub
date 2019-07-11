简介：
    本项目为两个模块YcApp和YcWeb
    
    注：
    YcApp：主要是用来做使用大数据租组件等的应用开发
    YcWeb：主要是开发web相关rest等接口使用
    
  
  
── conf
│   ├── server.properties
│   └── sql
├── logs
│   ├── errordata_2019070515.log
│   ├── server.log
│   └── server_error.log
├── pom.xml
├── resources
│   └── log4j.properties
├── src
│   ├── main
│   │   └── java
│   │       └── com
│   │           └── yicai
│   │               ├── Bean
│   │               │   ├── AnchorFans.java
│   │               │   ├── EventsMesgInfo.java
│   │               │   ├── FansInfo.java
│   │               │   ├── FansKafkaMesgInfo.java
│   │               │   └── KafkaMesgInfo.java
│   │               ├── DBopts
│   │               │   └── PGCopyInUtils.java
│   │               ├── ErrorUtils
│   │               │   ├── CopyInErrorToFile.java
│   │               │   └── HandelPGCopyinError.java
│   │               ├── Spark
│   │               │   ├── ConfigurationManager.java
│   │               │   ├── Constants.java
│   │               │   ├── EventsUserFlowWithSpark.java
│   │               │   ├── KafkaStreamingWordCount.java
│   │               │   └── SparkWordCount.java
│   │               ├── Utils
│   │               │   ├── ConfigurationManager.java
│   │               │   └── TimeManager.java
│   │               ├── kafka
│   │               │   ├── ConsumerRecordToBean.java
│   │               │   ├── EventsConsumers.java
│   │               │   ├── FansBatchConsumers.java
│   │               │   ├── FansConsumers.java
│   │               │   └── KafkaProducers.java
│   │               └── test
│   │                   ├── test_gp_conn.java
│   │                   ├── test_serializable.java
│   │                   ├── test_time.java
│   │                   └── testfile.java
└── target
    ├── java
    │   ├── Demokafka-0.0.1-SNAPSHOT.jar
    │   └── lib
    └── test-classes