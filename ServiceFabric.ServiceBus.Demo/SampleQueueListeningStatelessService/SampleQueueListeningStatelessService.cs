using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using System.Collections.Generic;
using System.Fabric;
using System.Threading;
using Microsoft.Azure;
using Microsoft.ServiceBus.Messaging;
using ServiceFabric.ServiceBus.Services;
using ServiceFabric.ServiceBus.Services.CommunicationListeners;

namespace SampleQueueListeningStatelessService
{
	/// <summary>
	/// The FabricRuntime creates an instance of this class for each service type instance. 
	/// </summary>
	internal sealed class SampleQueueListeningStatelessService : StatelessService
	{
		public SampleQueueListeningStatelessService(StatelessServiceContext serviceContext) : base(serviceContext)
		{
		}

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

	internal sealed class Handler : AutoCompleteServiceBusMessageReceiver
	{
		private readonly StatelessService _service;

		public Handler(StatelessService service)
		{
			_service = service;
		}

		protected override void ReceiveMessageImpl(BrokeredMessage message, CancellationToken cancellationToken)
		{
			ServiceEventSource.Current.ServiceMessage(_service, $"Handling queue message {message.MessageId}");
		}
	}
}
