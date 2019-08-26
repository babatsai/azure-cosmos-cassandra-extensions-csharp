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

namespace CosmosDnsAwareRRPolicyTest
{
    using System;
    using System.Net.Security;
    using System.Security.Authentication;
    using System.Security.Cryptography.X509Certificates;
    using System.Threading;
    using Cassandra;
    using CosmosDnsAwareRRPolicy;

    class CosmosDnsAwareRRPolicyTest
    {
        static readonly string contactPoint = "<CONTACT POINT>";
        static readonly string username = "<USERNAME>";
        static readonly string password = "<PASSWORD>";
        static readonly int port = 10350;

        public static void Main(string[] args)
        {
            var options = new Cassandra.SSLOptions(SslProtocols.Tls12, true, ValidateServerCertificate);
            options.SetHostNameResolver((ipAddress) => contactPoint);

            Cluster cluster = Cluster.Builder()
                                     .AddContactPoint(contactPoint)
                                     .WithCredentials(username, password)
                                     .WithPort(port)
                                     .WithSSL(options)
                                     .WithLoadBalancingPolicy(new CosmosDnsAwareRRPolicy(contactPoint))
                                     .Build();
            ISession session = cluster.Connect();
            string keyspaceName = "ks1";
            string tableName = "tb1";
            try
            {
                session.Execute($"CREATE KEYSPACE IF NOT EXISTS {keyspaceName} with foo='bar'");
                session.Execute($"CREATE TABLE IF NOT EXISTS {keyspaceName}.{tableName} (pk int PRIMARY KEY, r1 int)");
                int i = 0;
                for (; i < 100; i++)
                {
                    session.Execute($"INSERT INTO {keyspaceName}.{tableName} (pk, r1) VALUES ({i}, {i})");
                    Console.WriteLine($"Written row {i}");
                    Thread.Sleep(500);
                }

                Console.WriteLine("Please trigger a manual failover from CosmosDB portal. Once it is done, hit ENTER, and new requests will be routed to the new write region.");
                Console.ReadKey();
                for (; i < 200; i++)
                {
                    session.Execute($"INSERT INTO {keyspaceName}.{tableName} (pk, r1) VALUES ({i}, {i})");
                    Console.WriteLine($"Written row {i}");
                    Thread.Sleep(1000);
                }
            }
            finally
            {
                session.Execute("DROP KEYSPACE IF EXISTS ks1");
            }
        }

        public static bool ValidateServerCertificate(
            object sender,
            X509Certificate certificate,
            X509Chain chain,
            SslPolicyErrors sslPolicyErrors)
        {
            if (sslPolicyErrors == SslPolicyErrors.None)
                return true;

            Console.WriteLine("Certificate error: {0}", sslPolicyErrors);
            // Do not allow this client to communicate with unauthenticated servers.
            return false;
        }
    }

}
