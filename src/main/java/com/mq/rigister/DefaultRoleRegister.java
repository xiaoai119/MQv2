package com.mq.rigister;

import com.mq.common.role.RoleInfo;

import java.util.HashMap;

/**
 * Created By xfj on 2020/3/16
 */
public class DefaultRoleRegister implements RoleRegister {
  //在本地保有一份本地注册的角色列表
  HashMap<String,String> map=new HashMap<String,String>();

  @Override
  public void rigisterRole(RoleInfo roleInfo){
      if(roleInfo==null)return;
      map.put(roleInfo.getRole(),roleInfo.getName());
  }

}
