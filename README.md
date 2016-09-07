# Demo Project
Need some help to get started? Have a look at: 'https://github.com/loekd/ServiceFabric.ServiceBus/tree/master/ServiceFabric.ServiceBus.Demo'

## Nuget Packages:
ServiceFabric.ServiceBus.Clients
https://www.nuget.org/packages/ServiceFabric.ServiceBus.Clients
For communication to Service Fabric Reliable Services using the Communication Listener from the package 'ServiceFabric.ServiceBus.Services'.
Provides a ServiceBusTopicCommunicationClient to be used with 'ServicePartitionClient'.
*If you post messages to Service Bus in a different way, you won't need this client package.*

ServiceFabric.ServiceBus.Services
https://www.nuget.org/packages/ServiceFabric.ServiceBus.Services
For creating a Communication Listener that receives messages from Azure Service Bus (Queue/Subscription).

## Release notes:

v3.3.0
- Fixed a permissions issue that was introduced with session support. (Communication Listeners required 'manage' permissions.) (Found by Denis.)
  The option to use sessions is now a constructor argument for the Communication Listeners.
- Upgraded all nuget packages.

v3.2.2
- Merged pull request by cpletz that addresses an await issue.
- Upgraded all nuget packages.

v3.2.1	
- Added Session support and demo code that uses it. (requested by Aaron) 
- Upgraded all nuget packages.

v3.1.0	
- Upgraded all nuget packages. (Newer SDK version)
- Fixed async issue in AutoCompleteServiceBusMessageReceiver

v3.0.0	
- Handlers are now async to allow await in handling code. 
- Upgraded all nuget packages.
- Dispose BrokeredMessage after handling completes.

v2.0.0
- Upgraded to GA

v1.0.0
- First commit

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

	public Task ReceiveMessageAsync(BrokeredMessage message, CancellationToken cancellationToken)
        {
            ServiceEventSource.Current.ServiceMessage(_service, $"Handling subscription message {message.MessageId}");
            return Task.FromResult(true);
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
			, serviceBusQueueName), "StatefulService-ServiceBusQueueListener");
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
			, serviceBusSubscriptionName), "StatefulService-ServiceBusSubscriptionListener");
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
			, serviceBusQueueName), "StatelessService-ServiceBusQueueListener");
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
			, serviceBusSubscriptionName), "StatelessService-ServiceBusSubscriptionListener");
	}
}
```


# Client Package

Make sure your projects are configured to build as 64 bit programs!
----------------------------------------------


To communicate to a ServiceFabric Service through a Topic (Queues are also supported):
(add the Microsoft.ServiceFabric.Services nuget package)

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
