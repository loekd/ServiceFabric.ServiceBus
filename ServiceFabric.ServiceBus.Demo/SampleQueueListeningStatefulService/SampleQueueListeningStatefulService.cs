using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using System.Collections.Generic;
using System.Fabric;
using System.Threading;
using Microsoft.Azure;
using Microsoft.ServiceBus.Messaging;
using Microsoft.ServiceFabric.Data;
using ServiceFabric.ServiceBus.Services;
using ServiceFabric.ServiceBus.Services.CommunicationListeners;

namespace SampleQueueListeningStatefulService
{
	/// <summary>
	/// The FabricRuntime creates an instance of this class for each service type instance.
	/// </summary>
	internal sealed class SampleQueueListeningStatefulService : StatefulService
	{
		public SampleQueueListeningStatefulService(StatefulServiceContext serviceContext) : base(serviceContext)
		{
		}

		public SampleQueueListeningStatefulService(StatefulServiceContext serviceContext, IReliableStateManagerReplica reliableStateManagerReplica) : base(serviceContext, reliableStateManagerReplica)
		{
		}

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
}
