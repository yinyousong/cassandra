/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

/**
 * This Replication Strategy takes a property file that gives the intended
 * replication factor in each datacenter.  The sum total of the datacenter
 * replication factor values should be equal to the keyspace replication
 * factor.
 * <p/>
 * So for example, if the keyspace replication factor is 6, the
 * datacenter replication factors could be 3, 2, and 1 - so 3 replicas in
 * one datacenter, 2 in another, and 1 in another - totalling 6.
 * <p/>
 * This class also caches the Endpoints and invalidates the cache if there is a
 * change in the number of tokens.
 */
public class NetworkTopologyStrategy extends AbstractReplicationStrategy
{
    private final IEndpointSnitch snitch;
    private final Map<String, Integer> datacenters;
    private static final Logger logger = LoggerFactory.getLogger(NetworkTopologyStrategy.class);

    public NetworkTopologyStrategy(String table, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions) throws ConfigurationException
    {
        super(table, tokenMetadata, snitch, configOptions);
        this.snitch = snitch;

        Map<String, Integer> newDatacenters = new HashMap<String, Integer>();
        if (configOptions != null)
        {
            for (Entry<String, String> entry : configOptions.entrySet())
            {
                String dc = entry.getKey();
                if (dc.equalsIgnoreCase("replication_factor"))
                    throw new ConfigurationException("replication_factor is an option for SimpleStrategy, not NetworkTopologyStrategy");
                Integer replicas = Integer.valueOf(entry.getValue());
                newDatacenters.put(dc, replicas);
            }
        }

        datacenters = Collections.unmodifiableMap(newDatacenters);
        logger.debug("Configured datacenter replicas are {}", FBUtilities.toString(datacenters));
    }

    /**
     * TODO: This is O(N) but could be constant time if the snitch could tell us the complete topology in one call
     * @param dcEndpoints (out) this map is populated with dc -> set of endpoints
     * @return map of DC onto set of racks in that DC
     */
    @SuppressWarnings("serial")
    public Map<String, Set<String>> getRacks(TokenMetadata metadata, Map<String, Set<InetAddress>> dcEndpoints)
    {
        Map<String, Set<String>> racks = new HashMap<String, Set<String>>(datacenters.size())
        {{
            for (String dc : datacenters.keySet())
                put(dc, new HashSet<String>());
        }};
        for (InetAddress ep : metadata.getNormalEndpoints())
        {
            racks.get(snitch.getDatacenter(ep)).add(snitch.getRack(ep));
            dcEndpoints.get(snitch.getDatacenter(ep)).add(ep);
        }
        // remove empty DCs
        for (Iterator<Map.Entry<String, Set<InetAddress>>> it = dcEndpoints.entrySet().iterator();
                it.hasNext(); )
            if (it.next().getValue().isEmpty())
                it.remove();
        return racks;
    }

    /**
     * calculate endpoints in one pass through the tokens by tracking our progress in each DC, rack etc.
     * this is worst-case O(N) but should in most cases be ~ O(RF)
     */
    @SuppressWarnings("serial")
    @Override
    public List<InetAddress> calculateNaturalEndpoints(Token searchToken, TokenMetadata tokenMetadata)
    {
        // replicas we have found in each DC
        Map<String, List<InetAddress>> dcEndpoints = new HashMap<String, List<InetAddress>>(datacenters.size())
        {{
            for (Map.Entry<String, Integer> dc : datacenters.entrySet())
                put(dc.getKey(), new ArrayList<InetAddress>(dc.getValue()));
        }};
        // tracks how many endpoints are remaining for a given DC
        // when the set size is 0 we can remove that DC as we know we won't find any more replicas for it
        Map<String, Set<InetAddress>> remainingEndpoints = new HashMap<String, Set<InetAddress>>(datacenters.size())
        {{
            for (Map.Entry<String, Integer> dc : datacenters.entrySet())
                put(dc.getKey(), new HashSet<InetAddress>(dc.getValue()));
        }};
        // track which racks haven't yet been used for a DC
        // when the set size is 0 we know we can stop trying to get unique racks
        // (getRacks also populates remainingEndpoints)
        Map<String, Set<String>> racks = getRacks(tokenMetadata, remainingEndpoints);
        // tracks the endpoints that we skipped over while looking for unique racks
        // when we relax the rack uniqueness we can append this to the current result so we don't have to wind back the iterator
        Map<String, ArrayList<InetAddress>> skippedDcEndpoints = new HashMap<String, ArrayList<InetAddress>>(datacenters.size())
        {{
            for (Map.Entry<String, Integer> dc : datacenters.entrySet())
                put(dc.getKey(), new ArrayList<InetAddress>(dc.getValue()));
        }};
        Iterator<Token> tokenIter = TokenMetadata.ringIterator(tokenMetadata.sortedTokens(), searchToken, false);
        while (tokenIter.hasNext() && !remainingEndpoints.isEmpty())
        {
            Token next = tokenIter.next();
            InetAddress ep = tokenMetadata.getEndpoint(next);
            String dc = snitch.getDatacenter(ep);
            // have we already found all replicas for this dc or used this endpoint already?
            if (!remainingEndpoints.containsKey(dc) || !remainingEndpoints.get(dc).contains(ep))
                continue;
            // can we skip checking the rack?
            if (racks.get(dc).isEmpty())
            {
                if (remainingEndpoints.get(dc).remove(ep))
                    dcEndpoints.get(dc).add(ep);
            } else
            {
                String rack = snitch.getRack(ep);
                // is this a new rack?
                if (racks.get(dc).remove(rack))
                {
                    if (remainingEndpoints.get(dc).remove(ep))
                        dcEndpoints.get(dc).add(ep);
                    // if we've run out of distinct racks, add the hosts we skipped past already (up to RF)
                    if (racks.get(dc).isEmpty() && !skippedDcEndpoints.get(dc).isEmpty())
                    {
                        dcEndpoints.get(dc).addAll(skippedDcEndpoints.get(dc).subList(0, getReplicationFactor(dc) - dcEndpoints.get(dc).size()));
                        remainingEndpoints.get(dc).removeAll(skippedDcEndpoints.get(dc));
                    }
                } else
                {
                    if (remainingEndpoints.get(dc).remove(ep))
                        skippedDcEndpoints.get(dc).add(ep);
                }
            }
            // check if we found RF replicas for this DC or whether we exhausted its endpoints
            if (dcEndpoints.get(dc).size() == getReplicationFactor(dc) || remainingEndpoints.get(dc).isEmpty())
                remainingEndpoints.remove(dc);
        }

        List<InetAddress> replicas = new ArrayList<InetAddress>();
        for (Map.Entry<String, List<InetAddress>> endpoints : dcEndpoints.entrySet())
        {
            if (logger.isDebugEnabled())
                logger.debug("{} endpoints in datacenter {} for token {} ",
                        new Object[] { StringUtils.join(endpoints.getValue(), ","), endpoints.getKey(), searchToken});
            replicas.addAll(endpoints.getValue());
        }
        return replicas;
    }

    public int getReplicationFactor()
    {
        int total = 0;
        for (int repFactor : datacenters.values())
            total += repFactor;
        return total;
    }

    public int getReplicationFactor(String dc)
    {
        return datacenters.get(dc);
    }

    public Set<String> getDatacenters()
    {
        return datacenters.keySet();
    }

    public void validateOptions() throws ConfigurationException
    {
        for (Entry<String, String> e : this.configOptions.entrySet())
        {
            validateReplicationFactor(e.getValue());
        }

    }
}
