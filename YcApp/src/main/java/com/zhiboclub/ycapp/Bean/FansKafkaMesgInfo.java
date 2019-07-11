package com.zhiboclub.ycapp.Bean;

import lombok.Data;

import java.io.Serializable;

@Data
public class FansKafkaMesgInfo implements Serializable {

    private FansInfo fansInfo;

    private AnchorFans anchorFans;
}
