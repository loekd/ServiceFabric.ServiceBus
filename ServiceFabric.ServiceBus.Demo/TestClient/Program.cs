using System;
using System.Threading;
using Microsoft.Azure;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Microsoft.ServiceFabric.Services.Client;
using Microsoft.ServiceFabric.Services.Communication.Client;
using ServiceFabric.ServiceBus.Clients;
using System.Fabric;

namespace TestClient
{
    internal class Program
    {
        private static readonly string ConnectionStringForManaging;

        private static readonly string QueueNameStateless = "TestQueueStateless";
        private static readonly string TopicNameStateless = "TestTopicStateless";
        private static readonly string SubscriptionNameStateless = "TestSubscriptionStateless";

        private static readonly string QueueNameStateful = "TestQueueStateful";
        private static readonly string TopicNameStateful = "TestTopicStateful";
        private static readonly string SubscriptionNameStateful = "TestSubscriptionStateful";

        static Program()
        {
            //Get a Service Bus connection string that has rights to manage the Service Bus namespace, to be able to create queues and topics.
            //this is not needed in production situations, unless you want to create them on the fly using your own code.
            ConnectionStringForManaging = CloudConfigurationManager.GetSetting("Microsoft.ServiceBus.ConnectionString.Manage");
        }

        // ReSharper disable once UnusedParameter.Local
        private static void Main(string[] args)
        {
            try
            {
                ProcessInput();
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex);
                Console.WriteLine("Hit any key to exit");
                Console.ReadKey(true);
            }
        }

        private static void ListEndpoints()
        {
            var resolver = ServicePartitionResolver.GetDefault();
            var fabricClient = new FabricClient();
            var apps = fabricClient.QueryManager.GetApplicationListAsync().Result;
            foreach (var app in apps)
            {
                Console.WriteLine($"Discovered application:'{app.ApplicationName}");

                var services = fabricClient.QueryManager.GetServiceListAsync(app.ApplicationName).Result;
                foreach (var service in services)
                {
                    Console.WriteLine($"Discovered Service:'{service.ServiceName}");

                    var partitions = fabricClient.QueryManager.GetPartitionListAsync(service.ServiceName).Result;
                    foreach (var partition in partitions)
                    {
                        Console.WriteLine($"Discovered Service Partition:'{partition.PartitionInformation.Kind} {partition.PartitionInformation.Id}");


                        ServicePartitionKey key;
                        switch (partition.PartitionInformation.Kind)
                        {
                            case ServicePartitionKind.Singleton:
                                key = ServicePartitionKey.Singleton;
                                break;
                            case ServicePartitionKind.Int64Range:
                                var longKey = (Int64RangePartitionInformation)partition.PartitionInformation;
                                key = new ServicePartitionKey(longKey.LowKey);
                                break;
                            case ServicePartitionKind.Named:
                                var namedKey = (NamedPartitionInformation)partition.PartitionInformation;
                                key = new ServicePartitionKey(namedKey.Name);
                                break;
                            default:
                                throw new ArgumentOutOfRangeException("partition.PartitionInformation.Kind");
                        }
                        var resolved = resolver.ResolveAsync(service.ServiceName, key, CancellationToken.None).Result;
                        foreach (var endpoint in resolved.Endpoints)
                        {
                            Console.WriteLine($"Discovered Service Endpoint:'{endpoint.Address}");
                        }
                    }
                }
            }
        }

        private static void ProcessInput()
        {
            while (true)
            {
                Console.WriteLine("Choose an option:");

                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("Manage Azure Service Bus namespace:");
                Console.WriteLine("1: Create the demo service bus queues");
                Console.WriteLine("2: Create the demo service bus topics");
                Console.WriteLine("3: Create subscriptions for the topics created with option 2");

                Console.ResetColor();
                Console.WriteLine();
                Console.WriteLine("Send Messages to Reliable Services:");

                Console.WriteLine("4: Send a message to SampleQueueListeningStatefulService");
                Console.WriteLine("\t--> (uses sessions, receives separate messages)");

                Console.WriteLine("5: Send a message to SampleQueueListeningStatelessService");
                Console.WriteLine("\t--> (uses AutoRenewTimeout, receives separate messages)");


                Console.WriteLine("6: Send a message to SampleSubscriptionListeningStatefulService.");
                Console.WriteLine("\t--> (uses sessions, receives batches)");

                Console.WriteLine("7: Send a message to SampleSubscriptionListeningStatelessService");
                Console.WriteLine("\t--> (lock renew, receives batches)");

                Console.WriteLine("L: List services and endpoints");
                Console.WriteLine();
                Console.WriteLine("Other: exit");


                var key = Console.ReadKey(true);
                switch (key.Key)
                {
                    case ConsoleKey.D1:
                    case ConsoleKey.NumPad1:
                        CreateServiceBusQueue(QueueNameStateless);
                        CreateServiceBusQueue(QueueNameStateful, true);
                        break;

                    case ConsoleKey.D2:
                    case ConsoleKey.NumPad2:
                        CreateServiceBusTopic(TopicNameStateless);
                        CreateServiceBusTopic(TopicNameStateful);
                        break;

                    case ConsoleKey.D3:
                    case ConsoleKey.NumPad3:
                        CreateTopicSubscription(TopicNameStateless, SubscriptionNameStateless);
                        CreateTopicSubscription(TopicNameStateful, SubscriptionNameStateful, true);
                        break;

                    case ConsoleKey.D4:
                    case ConsoleKey.NumPad4:
                        SendTestMessageToQueue(new Uri("fabric:/MyServiceFabricApp/SampleQueueListeningStatefulService"), QueueNameStateful, true, true);
                        break;

                    case ConsoleKey.D5:
                    case ConsoleKey.NumPad5:
                        SendTestMessageToQueue(new Uri("fabric:/MyServiceFabricApp/SampleQueueListeningStatelessService"), QueueNameStateless, false);
                        break;

                    case ConsoleKey.D6:
                    case ConsoleKey.NumPad6:
                        SendTestMessageToTopic(new Uri("fabric:/MyServiceFabricApp/SampleSubscriptionListeningStatefulService"), TopicNameStateful, true, true);
                        break;

                    case ConsoleKey.D7:
                    case ConsoleKey.NumPad7:
                        SendTestMessageToTopic(new Uri("fabric:/MyServiceFabricApp/SampleSubscriptionListeningStatelessService"), TopicNameStateless, false);
                        break;

                    case ConsoleKey.L:
                        ListEndpoints();
                        break;
                    default:
                        return;
                }

                Thread.Sleep(200);
                Console.Clear();
            }
        }

        private static void CreateServiceBusTopic(string topicName)
        {
            var namespaceManager = NamespaceManager.CreateFromConnectionString(ConnectionStringForManaging);

            if (namespaceManager.TopicExists(topicName))
            {
                Console.WriteLine($"Topic '{topicName}' exists.");
                return;
            }

            var td = new TopicDescription(topicName);
            var sendKey = SharedAccessAuthorizationRule.GenerateRandomKey();
            var receiveKey = SharedAccessAuthorizationRule.GenerateRandomKey();

            td.Authorization.Add(new SharedAccessAuthorizationRule("SendKey", sendKey, new[] { AccessRights.Send }));
            td.Authorization.Add(new SharedAccessAuthorizationRule("ReceiveKey", receiveKey, new[] { AccessRights.Listen }));

            namespaceManager.CreateTopic(td);
            Console.WriteLine($"Created Topic '{topicName}'.");

            Console.WriteLine($"Now manually update the App.config in the Subscription Listening-Service with the Send and Receive connection strings for this Topic:'{topicName}'.");
            Console.WriteLine("Send Key - SharedAccessKeyName:'SendKey'");
            Console.WriteLine($"Send Key - SharedAccessKey:'{sendKey}'");
            Console.WriteLine();

            Console.WriteLine($"Receive Key - SharedAccessKeyName:'ReceiveKey'");
            Console.WriteLine($"Receive Key - SharedAccessKey:'{receiveKey}'");

            Console.WriteLine("Hit any key to continue...");
            Console.ReadKey(true);
        }

        private static void CreateTopicSubscription(string topicName, string subscriptionName, bool requireSessions = false)
        {
            var namespaceManager = NamespaceManager.CreateFromConnectionString(ConnectionStringForManaging);
            if (namespaceManager.SubscriptionExists(topicName, subscriptionName))
            {
                if (namespaceManager.GetSubscription(topicName, subscriptionName).RequiresSession != requireSessions)
                {
                    Console.WriteLine($"Subscription '{subscriptionName}' will be deleted. Hit <enter> to confirm.");
                    Console.ReadLine();
                    namespaceManager.DeleteSubscription(topicName, subscriptionName);
                    Thread.Sleep(5000);
                }
                else
                {
                    Console.WriteLine($"Subscription '{subscriptionName}' for Topic '{topicName}' exists.");
                    return;
                }
            }
            var description = new SubscriptionDescription(topicName, subscriptionName)
            {
                RequiresSession = requireSessions
            };
            namespaceManager.CreateSubscription(description);

            Console.WriteLine($"Created Subscription '{subscriptionName}' for Topic '{topicName}'.");
        }

        private static void CreateServiceBusQueue(string queueName, bool requireSessions = false)
        {
            var namespaceManager = NamespaceManager.CreateFromConnectionString(ConnectionStringForManaging);

            if (namespaceManager.QueueExists(queueName))
            {
                if (namespaceManager.GetQueue(queueName).RequiresSession != requireSessions)
                {
                    Console.WriteLine($"Queue '{queueName}' will be deleted. Hit <enter> to confirm.");
                    Console.ReadLine();
                    namespaceManager.DeleteQueue(queueName);
                    Thread.Sleep(5);
                }
                else
                {
                    Console.WriteLine($"Queue '{queueName}' exists.");
                    return;
                }
            }

            var qd = new QueueDescription(queueName);
            var sendKey = SharedAccessAuthorizationRule.GenerateRandomKey();
            var receiveKey = SharedAccessAuthorizationRule.GenerateRandomKey();
            qd.Authorization.Add(new SharedAccessAuthorizationRule("SendKey", sendKey, new[] { AccessRights.Send }));
            qd.Authorization.Add(new SharedAccessAuthorizationRule("ReceiveKey", receiveKey, new[] { AccessRights.Listen }));
            qd.RequiresSession = requireSessions;

            namespaceManager.CreateQueue(qd);
            Console.WriteLine($"Created queue '{queueName}'.");

            Console.WriteLine($"Now manually update the App.config in the Queue Listening-Service with the Send and Receive connection strings for this Queue:'{queueName}'.");
            Console.WriteLine("Send Key - SharedAccessKeyName:'SendKey'");
            Console.WriteLine($"Send Key - SharedAccessKey:'{sendKey}'");
            Console.WriteLine();

            Console.WriteLine($"Receive Key - SharedAccessKeyName:'ReceiveKey'");
            Console.WriteLine($"Receive Key - SharedAccessKey:'{receiveKey}'");

            Console.WriteLine("Hit any key to continue...");
            Console.ReadKey(true);
        }


        private static void SendTestMessageToTopic(Uri uri, string topicName, bool serviceSupportsPartitions, bool requireSessions = false)
        {
            //the name of your application and the name of the Service, the default partition resolver and the topic name
            //to create a communication client factory:
            var resolver = ServicePartitionResolver.GetDefault();
            var factory = new ServiceBusTopicCommunicationClientFactory(resolver, null);

            ServicePartitionClient<ServiceBusTopicCommunicationClient> servicePartitionClient;

            if (serviceSupportsPartitions)
            {
                //determine the partition and create a communication proxy
                var partitionKey = new ServicePartitionKey(0L);
                servicePartitionClient = new ServicePartitionClient<ServiceBusTopicCommunicationClient>(factory, uri, partitionKey);
            }
            else
            {
                servicePartitionClient = new ServicePartitionClient<ServiceBusTopicCommunicationClient>(factory, uri);
            }

            //use the proxy to send a message to the Service
            servicePartitionClient.InvokeWithRetry(c => c.SendMessage(CreateMessage(requireSessions)));

            Console.WriteLine($"Message sent to topic '{topicName}'");
        }

        private static void SendTestMessageToQueue(Uri uri, string queueName, bool serviceSupportsPartitions, bool requireSessions = false)
        {
            //the name of your application and the name of the Service, the default partition resolver and the topic name
            //to create a communication client factory:
            var factory = new ServiceBusQueueCommunicationClientFactory(ServicePartitionResolver.GetDefault(), null);

            ServicePartitionClient<ServiceBusQueueCommunicationClient> servicePartitionClient;

            if (serviceSupportsPartitions)
            {
                //determine the partition and create a communication proxy
                var partitionKey = new ServicePartitionKey(0L);
                servicePartitionClient = new ServicePartitionClient<ServiceBusQueueCommunicationClient>(factory, uri, partitionKey);
            }
            else
            {
                servicePartitionClient = new ServicePartitionClient<ServiceBusQueueCommunicationClient>(factory, uri);
            }

            //use the proxy to send a message to the Service
            servicePartitionClient.InvokeWithRetry(c => c.SendMessage(CreateMessage(requireSessions)));

            Console.WriteLine($"Message sent to queue '{queueName}'");
        }

        private static BrokeredMessage CreateMessage(bool requireSessions)
        {
            var message = new BrokeredMessage()
            {
                Properties =
                {
                    { "TestKey", "TestValue" }
                }
            };

            if (requireSessions)
            {
                if ((++_messagesInSession) >= _maxMessagesInSession)
                {
                    _messagesInSession = 0;
                    _messageSessionId = Guid.NewGuid();
                }
                message.SessionId = _messageSessionId.ToString("N");
            }
            return message;
        }

        private static Guid _messageSessionId = Guid.NewGuid();
        private static int _messagesInSession;
        private const int _maxMessagesInSession = 5;
    }
}
