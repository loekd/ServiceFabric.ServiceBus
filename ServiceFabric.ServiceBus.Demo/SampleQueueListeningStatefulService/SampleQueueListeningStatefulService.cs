using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using System.Collections.Generic;
using Microsoft.Azure;
using Microsoft.ServiceBus.Messaging;
using ServiceFabric.ServiceBus.Services;
using ServiceFabric.ServiceBus.Services.CommunicationListeners;

namespace SampleQueueListeningStatefulService
{
	/// <summary>
	/// The FabricRuntime creates an instance of this class for each service type instance.
	/// </summary>
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
}
