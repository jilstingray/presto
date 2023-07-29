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

import com.facebook.airlift.discovery.client.Announcer;
import com.facebook.airlift.discovery.client.ServiceAnnouncement;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.spi.ConnectorId;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;

import javax.inject.Inject;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static com.google.common.base.Strings.nullToEmpty;

public class DynamicCatalogStore
{
    private static final Logger log = Logger.get(DynamicCatalogStore.class);
    private final Announcer announcer;
    private final AtomicBoolean catalogsLoaded = new AtomicBoolean();
    private final AtomicBoolean catalogsLoading = new AtomicBoolean();
    private final ConnectorManager connectorManager;
    private final DynamicCatalogStoreConfig config;
    private final InternalNodeManager nodeManager;

    @Inject
    public DynamicCatalogStore(ConnectorManager connectorManager,
            DynamicCatalogStoreConfig config,
            Announcer announcer,
            InternalNodeManager nodeManager)
    {
        this.connectorManager = connectorManager;
        this.config = config;
        this.announcer = announcer;
        this.nodeManager = nodeManager;
    }

    @SuppressWarnings("DuplicatedCode")
    private static void updateConnectorIdAnnouncement(Announcer announcer, ConnectorId connectorId, InternalNodeManager nodeManager)
    {
        //
        // This code was copied from PrestoServer, and is a hack that should be removed when the connectorId property is removed
        //

        // get existing announcement
        ServiceAnnouncement announcement = getPrestoAnnouncement(announcer.getServiceAnnouncements());

        // update connectorIds property
        Map<String, String> properties = new LinkedHashMap<>(announcement.getProperties());
        String property = nullToEmpty(properties.get("connectorIds"));
        Set<String> connectorIds = new LinkedHashSet<>(Splitter.on(',').trimResults().omitEmptyStrings().splitToList(property));
        connectorIds.add(connectorId.toString());
        properties.put("connectorIds", Joiner.on(',').join(connectorIds));

        // update announcement
        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(serviceAnnouncement(announcement.getType()).addProperties(properties).build());
        announcer.forceAnnounce();

        nodeManager.refreshNodes();
    }

    private static ServiceAnnouncement getPrestoAnnouncement(Set<ServiceAnnouncement> announcements)
    {
        for (ServiceAnnouncement announcement : announcements) {
            if (announcement.getType().equals("presto")) {
                return announcement;
            }
        }
        throw new RuntimeException("Presto announcement not found: " + announcements);
    }

    public boolean areCatalogsLoaded()
    {
        return catalogsLoaded.get();
    }

    public boolean isDynamicEnabled()
    {
        return config.getEnabled();
    }

    public void loadCatalogs()
    {
        if (catalogsLoading.compareAndSet(false, true)) {
            CuratorFramework curatorFramework = config.getCuratorFramework();
            TreeCache cache = TreeCache.newBuilder(curatorFramework, config.getZkPath()).setCacheData(false).build();
            try {
                cache.getListenable().addListener((client, event) -> {
                    switch (event.getType()) {
                        case NODE_ADDED:
                            createCatalog(event.getData());
                            break;
                        case NODE_UPDATED:
                            updateCatalog(event.getData(), event.getData());
                            break;
                        case NODE_REMOVED:
                            removeCatalog(event.getData());
                            break;
                        default:
                            break;
                    }
                });
                cache.start();
            }
            catch (Exception e) {
                log.error(e, "Error while loading catalogs");
            }
            catalogsLoaded.set(true);
        }
    }

    private void createCatalog(ChildData data)
    {
        if (data.getPath().equals(config.getZkPath())) {
            return;
        }
        try {
            CatalogInfo catalogInfo = new ObjectMapper().readValue(data.getData(), CatalogInfo.class);
            ConnectorId connectorId = connectorManager.createConnection(catalogInfo.getCatalogName(), catalogInfo.getConnectorName(), catalogInfo.getProperties());
            updateConnectorIdAnnouncement(announcer, connectorId, nodeManager);
        }
        catch (IOException e) {
            log.error(e, "Error while creating catalog");
        }
    }

    private void removeCatalog(ChildData data)
    {
        if (data.getPath().equals(config.getZkPath())) {
            return;
        }
        String catalogName = data.getPath().substring(data.getPath().lastIndexOf('/') + 1);
        connectorManager.dropConnection(catalogName);
    }

    private void updateCatalog(ChildData oldData, ChildData data)
    {
        removeCatalog(oldData);
        createCatalog(data);
    }
}
