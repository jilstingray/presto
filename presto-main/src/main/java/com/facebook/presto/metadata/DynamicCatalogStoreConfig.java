/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.metadata;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.log.Logger;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.ExponentialBackoffRetry;

import static java.util.Objects.requireNonNull;

public class DynamicCatalogStoreConfig
{
    private static final Logger log = Logger.get(DynamicCatalogStoreConfig.class);
    private static final Integer TIMEOUT = 30000;
    private static final Integer SLEEP_TIME = 3000;
    private static final Integer RETRY_TIMES = 3;
    private String enabled;
    private String zkAddress;
    private String zkPath;
    private String namespace;
    private CuratorFramework curatorFramework;

    public boolean getEnabled()
    {
        return enabled.equals("true");
    }

    @Config("catalog.dynamic.enabled")
    public DynamicCatalogStoreConfig setEnabled(String enabled)
    {
        this.enabled = enabled;
        return this;
    }

    public String getZkAddress()
    {
        return (zkAddress == null || zkAddress.isEmpty()) ? "127.0.0.1:2181" : zkAddress;
    }

    @Config("catalog.dynamic.zk.address")
    public DynamicCatalogStoreConfig setZkAddress(String zkAddress)
    {
        this.zkAddress = zkAddress;
        return this;
    }

    public String getZkPath()
    {
        return zkPath;
    }

    @Config("catalog.dynamic.zk.path")
    public DynamicCatalogStoreConfig setZkPath(String zkPath)
    {
        this.zkPath = zkPath;
        return this;
    }

    public String getNamespace()
    {
        return namespace;
    }

    @Config("catalog.dynamic.zk.namespace")
    public DynamicCatalogStoreConfig setNamespace(String namespace)
    {
        this.namespace = namespace;
        return this;
    }

    public CuratorFramework getCuratorFramework()
    {
        createCuratorFramework();
        return curatorFramework;
    }

    private synchronized void createCuratorFramework()
    {
        if (curatorFramework == null) {
            requireNonNull(zkAddress, "zookeeper address is null");
            requireNonNull(zkPath, "zookeeper path is null");
            log.info("get catalog from zookeeper, address: %s, path: %s", zkAddress, zkPath);
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(SLEEP_TIME, RETRY_TIMES);
            curatorFramework = org.apache.curator.framework.CuratorFrameworkFactory.builder().connectString(zkAddress).sessionTimeoutMs(TIMEOUT).connectionTimeoutMs(TIMEOUT).retryPolicy(retryPolicy).namespace(namespace).build();
            curatorFramework.start();
        }
    }
}
