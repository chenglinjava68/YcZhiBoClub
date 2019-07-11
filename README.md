一、简介：
    本项目为两个模块YcApp和YcWeb
    
    注：
    YcApp：主要是用来做使用大数据租组件等的应用开发
    YcWeb：主要是开发web相关rest等接口使用
    
  
二、项目目录结构

    logs            日志目录
    conf            配置目录
    src             源码目录
    resources       资源文件
    target          打包目录
    
    
三、打包运行

    1、导入依赖包和配置文件配置文件
    
    2、运行maven -> package打包，然后到使用命令后台运行
        nohup java -cp project.jar MainClass &
    