package com.zhiboclub.ycapp.Bean;

import lombok.Data;

import java.io.Serializable;
import java.sql.Timestamp;

@Data
public class AnchorFans implements Serializable {

    private static final long serialVersionUID = 1L;

    private String userId;
    private String anchorId;
    private Integer level;
    private Timestamp createTime;
    private Timestamp updateTime;
}
