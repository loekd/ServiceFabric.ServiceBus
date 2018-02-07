# Demo Project
Need some help to get started? Have a look at: 'https://github.com/loekd/ServiceFabric.ServiceBus/tree/master/ServiceFabric.ServiceBus.Demo'

## Nuget Packages:
Two packages, one for Receiving Brokered Messsages, and one (optional) for Sending them.

### ServiceFabric.ServiceBus.Services
https://www.nuget.org/packages/ServiceFabric.ServiceBus.Services
Contains implementations of `ICommunicationListener` that receive messages from Azure Service Bus (Queue/Subscription).

#### Receive from Queues:

- ServiceBusQueueBatchCommunicationListener
- ServiceBusQueueCommunicationListener

#### Receive from Subscriptions:

- ServiceBusSubscriptionBatchCommunicationListener
- ServiceBusSubscriptionCommunicationListener

Note: Session support is available too.

### ServiceFabric.ServiceBus.Clients
https://www.nuget.org/packages/ServiceFabric.ServiceBus.Clients
Receive BrokeredMessages in Service Fabric Reliable Services using the Communication Listener from the package 'ServiceFabric.ServiceBus.Services'.
Provides a ServiceBusTopicCommunicationClient to be used with 'ServicePartitionClient'.
*If you post messages to Service Bus in a different way, you won't need the client package.*

## Contribute!
Contributions are welcome.
Please upgrade the package version with a minor tick if there are no breaking changes. And add a line to the readme.md, stating the changes, e.g. 'upgraded to SF version x.y.z'.
Doing so will allow me to simply accept the PR, which will automatically trigger the release of a new package.
Please also make sure all feature additions have a corresponding unit test.

## Release notes:

v5.1.6
 - Upgraded nuget packages (SF 3.0.456)
 - Upgraded lowest netfx version to net452


v5.1.5
 - Upgraded nuget packages (SF 2.8.232)

v5.1.4
 - Upgraded nuget packages (SF 2.8.219)

v5.1.3
 - Upgraded nuget packages (SF 2.8.211)

v5.1.2
 - Upgraded nuget packages (SF 2.7.198)

v5.1.1
 - Fixed optional string not being optional issue reported by jfloodnet.

v5.1.0
 - Upgraded nuget packages (SF 2.6.220)

v5.0.0
 - Upgraded nuget packages (SF 2.6.210)
 - Upgraded sln to VS2017

v4.6.0
- Upgraded nuget packages (SF 2.5.216)

v4.5.1
- upgraded ASB Nuget package
- fixed throwing of an ArgumentNullException when not passsing optional ServiceContext. 

v4.5.0
- upgraded to new SDK and packages (2.4.164) 

v4.4.0
- upgraded to new SDK and packages (2.4.145) 

v4.3.1
- clarify documentation 

v4.3.0
- upgraded to new SDK and packages (2.3.311) 

v4.1.0
- upgraded to new SDK and packages (2.3.301) 

v4.0.0
- Some breaking changes in order to have full batch and session support. 
- For batch support use: ServiceBusQueueBatchCommunicationListener and ServiceBusSubscriptionBatchCommunicationListener, combined with IServiceBusMessageBatchReceiver. 
- For single message receive use: ServiceBusQueueCommunicationListener and ServiceBusSubscriptionCommunicationListener, combined with IServiceBusMessageReceiver.
- Message handlers now provide the MessageSession if available.
- Some properties are now obsolete to make naming more consistent.
- Single messages are now received using the message pump.
- Support entity path in connection strings, or explicit entitypath argument

v3.6.0
- Updated to new SDK, no other changes

v3.5.0
- Added logging support.
- Added automatic BrokeredMessage lock renewal option. 
  
  *How to use:* 
  
  Set the properties 'LogAction' and 'MessageLockRenewTimeSpan' on the Communication Listeners to enable:

  ``` csharp
  [..]
  Action<string> logAction = log => ServiceEventSource.Current.ServiceMessage(this, log);
  new ServiceInstanceListener(context => new ServiceBusQueueCommunicationListener(
				new Handler(logAction)
				, context
				, serviceBusQueueName
                , requireSessions: false)
			{
			    MessageLockRenewTimeSpan = TimeSpan.FromSeconds(50),  //auto renew every 50s, so processing can take longer than 60s (default lock duration).
                LogAction = logAction
			}, "StatelessService-ServiceBusQueueListener");
 [..]
  ```

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

*You can also use the ```AutoCompleteServiceBusMessageReceiver``` and ```AutoCompleteBatchServiceBusMessageReceiver``` handlers supplied by the package.*

```javascript
internal sealed class Handler : IServiceBusMessageReceiver
{
	private readonly StatefulService _service;

	public Handler(StatefulService service)
	{
		_service = service;
	}
	
	public Task ReceiveMessageAsync(BrokeredMessage message, MessageSession session, CancellationToken cancellationToken)
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
