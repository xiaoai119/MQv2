package com.rpc.server.register;

/**
 * Created By xfj on 2020/3/16
 */
public class SigletonZookeeperExportServiceRigister {
    private static ZookeeperExportServiceRegister instence;
    private SigletonZookeeperExportServiceRigister(){}
    public static synchronized ZookeeperExportServiceRegister getInstence(){
        if(instence==null)
            instence=new ZookeeperExportServiceRegister();
        return instence;
    }
}
