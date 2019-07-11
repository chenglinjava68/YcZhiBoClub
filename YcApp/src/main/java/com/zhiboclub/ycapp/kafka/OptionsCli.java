package com.zhiboclub.ycapp.kafka;

import org.apache.commons.cli.*;

public class OptionsCli {
    public String getConf2Cli(String[] args){
        Options options = new Options();
        //短选项，长选项，选项后是否有参数，描述
        Option option = new Option("c", "conf", true, "Please specify the configuration file");
        option.setRequired(false);//是否必须设置
        options.addOption(option);

        option = new Option("h", "help", false, "display help text");
        options.addOption(option);

        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException e) {
            new HelpFormatter().printHelp("java -cp project.jar MainClass", options, true);
            return "";
        }
        if (commandLine.hasOption('c')) {
            //获取参数
            String file = commandLine.getOptionValue('c');
            return file;
        }
        return null;
    }
}
