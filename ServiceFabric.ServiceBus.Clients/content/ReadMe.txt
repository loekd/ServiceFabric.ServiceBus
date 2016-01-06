Make sure your projects are configured to build as 64 bit programs!
----------------------------------------------


To communicate to a ServiceFabric Service:

//the name of your application and the name of the Service, the default partition resolver and the topic name
//to create a communication client factory:
var uri = new Uri("fabric:/[ServiceFabric App]/[ServiceFabric Service]");
var resolver = ServicePartitionResolver.GetDefault();
string serviceBusTopicName = CloudConfigurationManager.GetSetting("TopicName");
var factory = new ServiceBusTopicCommunicationClientFactory(resolver, serviceBusTopicName);

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