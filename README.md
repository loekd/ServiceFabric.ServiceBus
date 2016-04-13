# Demo Project
Need some help to get started? Have a look at: 'https://github.com/loekd/ServiceFabric.ServiceBus/tree/master/ServiceFabric.ServiceBus.Demo'

## Nuget Packages:
ServiceFabric.ServiceBus.Clients
https://www.nuget.org/packages/ServiceFabric.ServiceBus.Clients/2.0.0
ServiceFabric.ServiceBus.Services
https://www.nuget.org/packages/ServiceFabric.ServiceBus.Services/2.0.0

(It's in preview state, because it depends on the Service Fabric package, which is also in preview.)

### If you want to integrate this into an existing project, read on...
----------------------------------------------

# Server Package
Make sure your projects are configured to build as 64 bit programs!
----------------------------------------------

## To create a message handler:
```javascript
internal sealed class Handler : IServiceBusMessageReceiver
{
	private readonly StatefulService _service;

	public Handler(StatefulService service)
	{
		_service = service;
	}

	public void ReceiveMessage(BrokeredMessage message)
	{
		ServiceEventSource.Current.ServiceMessage(_service, $"Handling queue message {message.MessageId}");
	}
}
```
------------------------------------
## To create a Stateful Service that can be accessed through a Service Bus Queue:
```javascript
internal sealed class SampleQueueListeningStatefulService : StatefulService
{
	protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
	{
		// In the configuration file, define connection strings: 
		// "Microsoft.ServiceBus.ConnectionString.Receive"
		// and "Microsoft.ServiceBus.ConnectionString.Send"
			
		// Also, define a QueueName:
		string serviceBusQueueName = CloudConfigurationManager.GetSetting("QueueName");

		yield return new ServiceReplicaListener(context => new ServiceBusQueueCommunicationListener(
			new Handler(this)
			, context				
			, serviceBusQueueName), "ServiceBusEndPoint");
	}
}
```
------------------------------------
## To create a Stateful Service that can be accessed through a Service Bus Subscription:
```javascript
internal sealed class SampleSubscriptionListeningStatefulService : StatefulService
{
	protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
	{
		// In the configuration file, define connection strings: 
		// "Microsoft.ServiceBus.ConnectionString.Receive"
		// and "Microsoft.ServiceBus.ConnectionString.Send"
			
		// Also, define Topic & Subscription Names:
		string serviceBusTopicName = CloudConfigurationManager.GetSetting("TopicName");
		string serviceBusSubscriptionName = CloudConfigurationManager.GetSetting("SubscriptionName");

		yield return new ServiceReplicaListener(context => new ServiceBusSubscriptionCommunicationListener(
			new Handler(this)
			, context			
			, serviceBusTopicName
			, serviceBusSubscriptionName));
	}
}
```
------------------------------------
## To create a Stateless Service that can be accessed through a Service Bus Queue:
```javascript
internal sealed class SampleQueueListeningStatelessService : StatelessService
{
	protected override IEnumerable<ServiceInstanceListener> CreateServiceInstanceListeners()
	{
		// In the configuration file, define connection strings: 
		// "Microsoft.ServiceBus.ConnectionString.Receive"
		// and "Microsoft.ServiceBus.ConnectionString.Send"
			
		// Also, define a QueueName:
		string serviceBusQueueName = CloudConfigurationManager.GetSetting("QueueName");
		yield return new ServiceInstanceListener(context => new ServiceBusQueueCommunicationListener(
			new Handler(this)
			, context			
			, serviceBusQueueName));
	}
}
```
------------------------------------
## To create a Stateless Service that can be accessed through a Service Bus Subscription:
```javascript
internal sealed class SampleSubscriptionListeningStatelessService : StatelessService
{
	protected override IEnumerable<ServiceInstanceListener> CreateServiceInstanceListeners()
	{
		// In the configuration file, define connection strings: 
		// "Microsoft.ServiceBus.ConnectionString.Receive"
		// and "Microsoft.ServiceBus.ConnectionString.Send"
			
		// Also, define Topic & Subscription Names:
		string serviceBusTopicName = CloudConfigurationManager.GetSetting("TopicName");
		string serviceBusSubscriptionName = CloudConfigurationManager.GetSetting("SubscriptionName");

		yield return new ServiceInstanceListener(context => new ServiceBusSubscriptionCommunicationListener(
			new Handler(this)
			, context
			, serviceBusTopicName
			, serviceBusSubscriptionName));
	}
}
```


# Client Package

Make sure your projects are configured to build as 64 bit programs!
----------------------------------------------


To communicate to a ServiceFabric Service:

```javascript
//the name of your application and the name of the Service, the default partition resolver and the topic name
//to create a communication client factory:
var uri = new Uri("fabric:/[ServiceFabric App]/[ServiceFabric Service]");
var resolver = ServicePartitionResolver.GetDefault();
string serviceBusTopicName = CloudConfigurationManager.GetSetting("TopicName");
var factory = new ServiceBusTopicCommunicationClientFactory(resolver, serviceBusTopicName);

//determine the partition and create a communication proxy
var partitionKey = new ServicePartitionKey(0L);
var servicePartitionClient = new ServicePartitionClient<ServiceBusTopicCommunicationClient>(factory, uri, partitionKey);

//use the proxy to send a message to the Service
servicePartitionClient.InvokeWithRetry(c => c.SendMessage(new BrokeredMessage()
{
	Properties =
	{
		{ "TestKey", "TestValue" }
	}
}));
```
