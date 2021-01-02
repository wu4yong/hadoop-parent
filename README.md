this is my hadoop project！
Hadoop MR on yarn 搭建步骤：
1、etc/hadoop/mapred-site.xml:
	指定计算框架在yarn上运行：MR on YARN

	<property>
		<name>mapreduce.framework.name</name>
		<value>yarn</value>
	</property>

	
2、etc/hadoop/yarn-site.xml:
	给服务命名

		<property>
			<name>yarn.nodemanager.aux-services</name>
			<value>mapreduce_shuffle</value>
		</property>

3、etc/hadoop/yarn-site.xml:
<!-- 启用resourcemanager的ha -->
<property>
   <name>yarn.resourcemanager.ha.enabled</name>
   <value>true</value>
 </property>
 <!-- ha集群的名称是什么 -->
 <property>
   <name>yarn.resourcemanager.cluster-id</name>
   <value>cluster1</value>
 </property>
 <!-- 两个resourcemanager的逻辑名称是什么 -->
 <property>
   <name>yarn.resourcemanager.ha.rm-ids</name>
   <value>rm1,rm2</value>
 </property>
 <!-- 两个resourcemanager对应的主机名称或者ip地址 -->
 <property>
   <name>yarn.resourcemanager.hostname.rm1</name>
   <value>master1</value>
 </property>
 <property>
   <name>yarn.resourcemanager.hostname.rm2</name>
   <value>master2</value>
 </property>
 <!-- 用于存储HA状态的zookeeper集群地址，leader选举地址 -->
 <property>
   <name>yarn.resourcemanager.zk-address</name>
   <value>zk1:2181,zk2:2181,zk3:2181</value>
 </property>
	
	启动流程：
	1、启动zookeeper

	2、在node1启动
	     start-dfs.sh
	3、在node03启动yarn
		start-yarn.sh
	4、在node04 启动resourcemanager
	yarn-daemon.sh start resourcemanager
