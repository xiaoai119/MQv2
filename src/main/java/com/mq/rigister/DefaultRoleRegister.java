package com.mq.rigister;

import com.mq.common.role.RoleMetaInfo;

import java.util.HashMap;

/**
 * Created By xfj on 2020/3/16
 */
public class DefaultRoleRegister implements RoleRegister {
  //在本地保有一份本地注册的角色列表
  HashMap<String,String> map=new HashMap<String,String>();

  @Override
  public void rigisterRole(RoleMetaInfo roleMetaInfo){
      if(roleMetaInfo ==null)return;
      map.put(roleMetaInfo.getRole(), roleMetaInfo.getName());
  }

}
