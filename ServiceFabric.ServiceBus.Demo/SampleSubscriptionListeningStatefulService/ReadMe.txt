Make sure your projects are configured to build as 64 bit programs!
----------------------------------------------

To create a message handler:

internal sealed class Handler : AutoCompleteServiceBusMessageReceiver
{
	private readonly StatefulService _service;

	public Handler(StatefulService service)
	{
		_service = service;
	}

	protected override void ReceiveMessageImpl(BrokeredMessage message, CancellationToken cancellationToken)
	{
		ServiceEventSource.Current.ServiceMessage(_service, $"Handling queue message {message.MessageId}");
	}
}

------------------------------------
To create a Stateful Service that can be accessed through a Service Bus Queue:

internal sealed class SampleQueueListeningStatefulService : StatefulService
{
	protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
	{
		// In the configuration file, define connection strings: 
		// "Microsoft.ServiceBus.ConnectionString.Receive"
		// and "Microsoft.ServiceBus.ConnectionString.Send"
			
		// Also, define a QueueName:
		string serviceBusQueueName = CloudConfigurationManager.GetSetting("QueueName");

		yield return new ServiceReplicaListener(parameters => new ServiceBusQueueCommunicationListener(
			new Handler(this)
			, parameters				
			, serviceBusQueueName), "ServiceBusEndPoint");
	}
}

------------------------------------
To create a Stateful Service that can be accessed through a Service Bus Subscription:

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

		yield return new ServiceReplicaListener(parameters => new ServiceBusSubscriptionCommunicationListener(
			new Handler(this)
			, parameters			
			, serviceBusTopicName
			, serviceBusSubscriptionName));
	}
}

------------------------------------
To create a Stateless Service that can be accessed through a Service Bus Queue:

internal sealed class SampleQueueListeningStatelessService : StatelessService
{
	protected override IEnumerable<ServiceInstanceListener> CreateServiceInstanceListeners()
	{
		// In the configuration file, define connection strings: 
		// "Microsoft.ServiceBus.ConnectionString.Receive"
		// and "Microsoft.ServiceBus.ConnectionString.Send"
			
		// Also, define a QueueName:
		string serviceBusQueueName = CloudConfigurationManager.GetSetting("QueueName");
		yield return new ServiceInstanceListener(parameters => new ServiceBusQueueCommunicationListener(
			new Handler(this)
			, parameters			
			, serviceBusQueueName));
	}
}

------------------------------------
To create a Stateless Service that can be accessed through a Service Bus Subscription:

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

		yield return new ServiceInstanceListener(parameters => new ServiceBusSubscriptionCommunicationListener(
			new Handler(this)
			, parameters
			, serviceBusTopicName
			, serviceBusSubscriptionName));
	}
}