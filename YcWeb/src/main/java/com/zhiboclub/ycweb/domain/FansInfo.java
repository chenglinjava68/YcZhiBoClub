package com.zhiboclub.ycweb.domain;

import lombok.Data;

import java.io.Serializable;
import java.sql.Timestamp;

@Data
public class FansInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private String userId;
    private String userName;
    private String userAvatar;
    private Integer taoQiHi;
    private Boolean aPassUser;
    private Boolean vipUser;
    private Timestamp createTime;
    private Timestamp updateTime;

}
