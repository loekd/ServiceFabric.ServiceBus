using System;
using Microsoft.Azure;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Microsoft.ServiceFabric.Services.Client;
using Microsoft.ServiceFabric.Services.Communication.Client;
using ServiceFabric.ServiceBus.Clients;

namespace TestClient
{
	internal class Program
	{
		private static readonly string ConnectionStringForManaging;
		private static readonly string QueueName = "TestQueue";
		private static readonly string TopicName = "TestTopic";
		private static readonly string SubscriptionName = "TestSubscription";

		static Program()
		{
			//Get a Service Bus connection string that has rights to manage the Service Bus namespace, to be able to create queues and topics.
			//this is not needed in production situations, unless you want to create them on the fly using your own code.
			ConnectionStringForManaging = CloudConfigurationManager.GetSetting("Microsoft.ServiceBus.ConnectionString.Manage");

			string queueName = CloudConfigurationManager.GetSetting("QueueName");
			if (!string.IsNullOrWhiteSpace(queueName))
			{
				QueueName = queueName;
			}

			string topicName = CloudConfigurationManager.GetSetting("TopicName");
			if (!string.IsNullOrWhiteSpace(topicName))
			{
				TopicName = topicName;
			}

			string subscriptionName = CloudConfigurationManager.GetSetting("SubscriptionName");
			if (!string.IsNullOrWhiteSpace(subscriptionName))
			{
				SubscriptionName = subscriptionName;
			}
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

		private static void ProcessInput()
		{
			while (true)
			{
				Console.WriteLine("Choose an option:");
				Console.WriteLine("1: Create a service bus queue");
				Console.WriteLine("2: Send a message to the queue created with option 2.");

				Console.WriteLine("3: Create a service bus topic");
				Console.WriteLine("4: Create a subscription for the topic created with option 3");
				Console.WriteLine("5: Send a message to the topic created with option 3.");
				
				Console.WriteLine("Other: exit");


				var key = Console.ReadKey(true);
				switch (key.Key)
				{
					case ConsoleKey.D1:
					case ConsoleKey.NumPad1:
						CreateServiceBusQueue();
						break;

					case ConsoleKey.D2:
					case ConsoleKey.NumPad2:
						SendTestMessageToQueue();
						break;

					case ConsoleKey.D3:
					case ConsoleKey.NumPad3:
						CreateServiceBusTopic();
						break;
					case ConsoleKey.D4:
					case ConsoleKey.NumPad4:
						CreateTopicSubscription();
						break;

					case ConsoleKey.D5:
					case ConsoleKey.NumPad5:
						SendTestMessageToTopic();
						break;
						
					default:
						return;
				}
			}
		}

		private static void CreateServiceBusTopic()
		{
			var namespaceManager = NamespaceManager.CreateFromConnectionString(ConnectionStringForManaging);

			if (namespaceManager.TopicExists(TopicName))
			{
				Console.WriteLine($"Topic '{TopicName}' exists.");
				return;
			}

			var td = new TopicDescription(TopicName);
			td.Authorization.Add(new SharedAccessAuthorizationRule("SendKey", SharedAccessAuthorizationRule.GenerateRandomKey(), new[] { AccessRights.Send }));

			namespaceManager.CreateTopic(td);
			Console.WriteLine($"Created Topic '{TopicName}'.");
		}

		private static void CreateTopicSubscription()
		{
			var namespaceManager = NamespaceManager.CreateFromConnectionString(ConnectionStringForManaging);
			if (namespaceManager.SubscriptionExists(TopicName, SubscriptionName))
			{
				Console.WriteLine($"Subscription '{SubscriptionName}' for Topic '{TopicName}' exists.");
				return;
			}
			namespaceManager.CreateSubscription(TopicName, SubscriptionName);
			Console.WriteLine($"Created Subscription '{SubscriptionName}' for Topic '{TopicName}'.");
		}

		private static void CreateServiceBusQueue()
		{
			var namespaceManager = NamespaceManager.CreateFromConnectionString(ConnectionStringForManaging);

			if (namespaceManager.QueueExists(QueueName))
			{
				Console.WriteLine($"Queue '{QueueName}' exists.");
				return;
			}

			var qd = new QueueDescription(QueueName);
			qd.Authorization.Add(new SharedAccessAuthorizationRule("SendKey", SharedAccessAuthorizationRule.GenerateRandomKey(), new[] { AccessRights.Send }));

			namespaceManager.CreateQueue(qd);
			Console.WriteLine($"Created queue '{QueueName}'.");
		}

		private static void SendTestMessageToTopic()
		{
			//the name of your application and the name of the Service, the default partition resolver and the topic name
			//to create a communication client factory:
			var uri = new Uri("fabric:/MyServiceFabricApp/SampleSubscriptionListeningStatefulService");
			var resolver = ServicePartitionResolver.GetDefault();
			var factory = new ServiceBusTopicCommunicationClientFactory(resolver, TopicName);

			//determine the partition and create a communication proxy
			long partitionKey = 0L;
			var servicePartitionClient = new ServicePartitionClient<ServiceBusTopicCommunicationClient>(factory, uri, partitionKey);

			//use the proxy to send a message to the Service
			servicePartitionClient.InvokeWithRetry(c => c.SendMessage(new BrokeredMessage()
			{
				Properties =
				{
					{ "TestKey", "TestValue" }
				}
			}));

			Console.WriteLine("Message sent to topic");
		}

		private static void SendTestMessageToQueue()
		{
			//the name of your application and the name of the Service, the default partition resolver and the topic name
			//to create a communication client factory:
			var uri = new Uri("fabric:/MyServiceFabricApp/SampleQueueListeningStatefulService");
			var resolver = ServicePartitionResolver.GetDefault();
			var factory = new ServiceBusQueueCommunicationClientFactory(resolver, QueueName);

			//determine the partition and create a communication proxy
			long partitionKey = 0L;
			var servicePartitionClient = new ServicePartitionClient<ServiceBusQueueCommunicationClient>(factory, uri, partitionKey);

			//use the proxy to send a message to the Service
			servicePartitionClient.InvokeWithRetry(c => c.SendMessage(new BrokeredMessage()
			{
				Properties =
				{
					{ "TestKey", "TestValue" }
				}
			}));

			Console.WriteLine("Message sent to queue");
		}
	}
}
