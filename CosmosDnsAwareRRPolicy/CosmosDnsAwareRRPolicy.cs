/*
 * The MIT License (MIT)
 *
 * Copyright (c) Microsoft. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
 * Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

namespace CosmosDnsAwareRRPolicy
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Linq;
    using System.Net;
    using System.Net.Sockets;
    using Cassandra;

    /**
        * CosmosDB failover aware Round-robin load balancing policy.
        * This policy allows the user to seamlessly failover the default write region.
        * This is very similar to DCAwareRoundRobinPolicy, with a difference that
        * it considers the nodes in the default write region at distance LOCAL.
        * All other nodes are considered at distance REMOTE.
        * The nodes in the default write region are retrieved from the global endpoint
        * of the account, based on the dns refresh interval of 60 seconds by default.
        */
    public class CosmosDnsAwareRRPolicy : ILoadBalancingPolicy
    {
        private string _globalContactPoint;
        private readonly TimeSpan _dnsExpirationTimespan;
        private DateTime lastDnsLookupTime = DateTime.MinValue;

        private readonly int _maxIndex = Int32.MaxValue - 10000;
        private volatile Tuple<List<Host>, List<Host>> _hosts;
        private readonly object _hostCreationLock = new object();
        ICluster _cluster;
        int _index;

        /// <summary>
        ///  Creates a new datacenter aware round robin policy given the name of the local
        ///  datacenter. <p> The name of the local datacenter provided must be the local
        ///  datacenter name as known by Cassandra. </p><p> The policy created will ignore all
        ///  remote hosts. In other words, this is equivalent to 
        ///  <c>new CosmosDBPrimaryAwareRoundRobinPolicy(localDc, 0)</c>.</p>
        /// </summary>
        /// <param name="globalContactPoint"> the name of the local datacenter (as known by Cassandra).</param>
        /// <param name="dnsExpirationInSeconds"> The duration in which the address needs to be resolved by dns.</param>
        public CosmosDnsAwareRRPolicy(string globalContactPoint, int dnsExpirationInSeconds = 60)
        {
            _globalContactPoint = globalContactPoint;
            _dnsExpirationTimespan = TimeSpan.FromSeconds(dnsExpirationInSeconds);
        }

        private IPAddress[] LocalAddresses = null;

        private IPAddress[] GetLocalAddresses()
        {
            if (this.LocalAddresses == null || DateTime.UtcNow.Subtract(this.lastDnsLookupTime) > this._dnsExpirationTimespan)
            {
                try
                {
                    this.LocalAddresses = Dns.GetHostEntry(this._globalContactPoint).AddressList;
                    this.lastDnsLookupTime = DateTime.UtcNow;
                }
                catch (SocketException ex)
                {
                    // dns entry may be temporarily unavailable.
                    if (ex.Message.Contains("No such host is known"))
                    {
                        if (this.LocalAddresses == null)
                        {
                            throw new ArgumentException($"The dns could not resolve the globalContactPoint the first time.");
                        }
                    }
                }
            }

            return this.LocalAddresses;
        }

        public void Initialize(ICluster cluster)
        {
            _cluster = cluster;
            //When the pool changes, it should clear the local cache
            _cluster.HostAdded += _ => ClearHosts();
            _cluster.HostRemoved += _ => ClearHosts();

            //Check that the datacenter exists
            if (_cluster.AllHosts().FirstOrDefault(h => this.GetLocalAddresses().Contains(h.Address.Address)) == null)
            {
                var clusterAllHosts = string.Join(", ", _cluster.AllHosts().Select(h => h.Address));
                var globalContactPointHosts = string.Join(", ", (object[])this.GetLocalAddresses());
                throw new ArgumentException($"The cluster hosts {clusterAllHosts} don't have any of the globalContactPoint addresses: {globalContactPointHosts}");
            }
        }

        /// <summary>
        ///  Return the HostDistance for the provided host. 
        ///  <p> This policy consider nodes
        ///  in the default write region as <c>Local</c>. 
        ///  All other regions are considered as <c>Remote</c>.</p>
        /// </summary>
        /// <param name="host"> the host of which to return the distance of. </param>
        /// <returns>the HostDistance to <c>host</c>.</returns>
        public HostDistance Distance(Host host)
        {
            if (this.GetLocalAddresses().Contains(host.Address.Address))
            {
                return HostDistance.Local;
            }
            return HostDistance.Remote;
        }

        /// <summary>
        ///  Returns the hosts to use for a new query. <p> The returned plan will always try each known host in the default write region first, and then,
        ///  if none of the host is reachable, it will try all other regions.
        ///  The order of the local node in the returned query plan will follow a
        ///  Round-robin algorithm.</p>
        /// </summary>
        /// <param name="keyspace">Keyspace on which the query is going to be executed</param>
        /// <param name="query"> the query for which to build the plan. </param>
        /// <returns>a new query plan, i.e. an iterator indicating which host to try
        ///  first for querying, which one to use as failover, etc...</returns>
        public IEnumerable<Host> NewQueryPlan(string keyspace, IStatement query)
        {
            var startIndex = Interlocked.Increment(ref _index);
            //Simplified overflow protection
            if (startIndex > _maxIndex)
            {
                Interlocked.Exchange(ref _index, 0);
            }
            var hosts = GetHosts();
            var localHosts = hosts.Item1;
            var remoteHosts = hosts.Item2;
            //Round-robin through local nodes
            for (var i = 0; i < localHosts.Count; i++)
            {
                yield return localHosts[(startIndex + i) % localHosts.Count];
            }

            foreach (var h in remoteHosts)
            {
                yield return h;
            }
        }

        private void ClearHosts()
        {
            _hosts = null;
        }

        /// <summary>
        /// Gets a tuple containing the list of local and remote nodes
        /// </summary>
        internal Tuple<List<Host>, List<Host>> GetHosts()
        {
            var hosts = _hosts;
            if (hosts != null && DateTime.UtcNow.Subtract(this.lastDnsLookupTime) <= this._dnsExpirationTimespan)
            {
                return hosts;
            }
            lock (_hostCreationLock)
            {
                var localHosts = new List<Host>();
                var remoteHosts = new List<Host>();

                //Do not reorder instructions, the host list must be up to date now, not earlier
#if !NETCORE
                Thread.MemoryBarrier();
#else
            Interlocked.MemoryBarrier();
#endif
                //shallow copy the nodes
                var allNodes = _cluster.AllHosts().ToArray();
                HashSet<IPAddress> localAddresses = new HashSet<IPAddress>(this.GetLocalAddresses());

                //Split between local and remote nodes 
                foreach (var h in allNodes)
                {
                    if (localAddresses.Contains(h.Address.Address))
                    {
                        localHosts.Add(h);
                    }
                    else
                    {
                        remoteHosts.Add(h);
                    }
                }
                hosts = new Tuple<List<Host>, List<Host>>(localHosts, remoteHosts);
                _hosts = hosts;
            }
            return hosts;
        }
    }
}
