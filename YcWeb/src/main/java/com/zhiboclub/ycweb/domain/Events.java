package com.zhiboclub.ycweb.domain;

import lombok.Data;

import java.io.Serializable;
import java.sql.Timestamp;

@Data
public class Events implements Serializable {

    private static final long serialVersionUID = 1L;

    private String liveId;
    private String anchorId;
    private String topic;
    private String title;
    private Timestamp startTime;
    private String type;
    private Integer typeCode;
    private FansInfo user;
    private String body;
    private Timestamp createTime;
    private Timestamp updateTime;
}
