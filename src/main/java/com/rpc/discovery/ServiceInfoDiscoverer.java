package com.rpc.discovery;

import java.util.ArrayList;
import java.util.List;

public interface ServiceInfoDiscoverer {
	List<ServiceInfo> getServiceInfo(String name);
	default List<ServiceInfo> getServiceInfo(String name,String routeKey){return new ArrayList<>();}
}
