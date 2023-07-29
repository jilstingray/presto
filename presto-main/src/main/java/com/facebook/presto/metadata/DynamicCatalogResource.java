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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.util.DynamicCatalogJsonUtil;
import org.apache.zookeeper.CreateMode;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@Path("/v1/catalog")
public class DynamicCatalogResource
{
    private static final Logger log = Logger.get(DynamicCatalogResource.class);
    private final DynamicCatalogStoreConfig config;
    private final String zkPath;
    private final boolean enabled;

    @Inject
    public DynamicCatalogResource(DynamicCatalogStoreConfig config)
    {
        this.config = config;
        this.zkPath = config.getZkPath();
        this.enabled = config.getEnabled();
    }

    private void isEnabled()
    {
        if (!enabled) {
            log.info("Dynamic catalog is disabled");
            return;
        }
        log.info("Dynamic catalog is enabled");
    }

    @GET
    @Path("/list")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public List<CatalogInfo> list()
            throws Exception
    {
        isEnabled();
        try {
            List<String> list = config.getCuratorFramework().getChildren().forPath(zkPath);
            return list.stream().map(s -> {
                try {
                    byte[] bytes = config.getCuratorFramework().getData().forPath(zkPath + "/" + s);
                    return DynamicCatalogJsonUtil.toObject(new String(bytes), CatalogInfo.class);
                }
                catch (Exception e) {
                    log.error("Failed to get catalog info", e);
                    return null;
                }
            }).filter(Objects::nonNull).collect(Collectors.toList());
        }
        catch (Exception e) {
            log.error("Failed to list catalogs", e);
            return Collections.emptyList();
        }
    }

    @GET
    @Path("/{catalogName}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public CatalogInfo getCatalogInfo(@PathParam("catalogName") String catalogName)
    {
        isEnabled();
        try {
            byte[] bytes = config.getCuratorFramework().getData().forPath(zkPath + "/" + catalogName);
            return DynamicCatalogJsonUtil.toObject(new String(bytes), CatalogInfo.class);
        }
        catch (Exception e) {
            log.error("Failed to get catalog info", e);
            return null;
        }
    }

    @POST
    @Path("/save")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public void save(CatalogInfo catalogInfo)
    {
        isEnabled();
        try {
            requireNonNull(catalogInfo.getCatalogName(), "catalogName is null");
            requireNonNull(catalogInfo.getConnectorName(), "connectorName is null");
            requireNonNull(catalogInfo.getProperties(), "properties are null");
            config.getCuratorFramework().create().orSetData().creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(zkPath + "/" + catalogInfo.getCatalogName(),
                            requireNonNull(DynamicCatalogJsonUtil.toJson(catalogInfo)).getBytes(StandardCharsets.UTF_8));
        }
        catch (Exception e) {
            log.error("Failed to save catalog info", e);
        }
    }

    @DELETE
    @Path("/{catalogName}")
    public void remove(@PathParam("catalogName") String catalogName)
    {
        isEnabled();
        try {
            config.getCuratorFramework().delete().deletingChildrenIfNeeded().withVersion(-1).forPath(zkPath + "/" + catalogName);
        }
        catch (Exception e) {
            log.error("Failed to remove catalog info", e);
        }
    }
}
