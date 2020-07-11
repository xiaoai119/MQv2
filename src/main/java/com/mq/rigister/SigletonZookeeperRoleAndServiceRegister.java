//package com.mq.rigister;
//
///**
// * Created By xfj on 2020/3/16
// */
//public class SigletonZookeeperRoleAndServiceRegister {
//    private static ZookeeperRoleAndServiceInfoRegister instance;
//    private SigletonZookeeperRoleAndServiceRegister(){}
//    public static ZookeeperRoleAndServiceInfoRegister getInstance() {
//        if (instance == null) {
//            synchronized(SigletonZookeeperRoleAndServiceRegister.class){
//                if(instance==null)
//                    instance = new ZookeeperRoleAndServiceInfoRegister();
//            }
//        }
//        return instance;
//    }
//}
