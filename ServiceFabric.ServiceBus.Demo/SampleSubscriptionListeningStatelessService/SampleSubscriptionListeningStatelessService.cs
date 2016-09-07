using System.Collections.Generic;
using System.Fabric;
using System.Threading;
using Microsoft.Azure;
using Microsoft.ServiceBus.Messaging;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using ServiceFabric.ServiceBus.Services;
using ServiceFabric.ServiceBus.Services.CommunicationListeners;
using System.Threading.Tasks;

namespace SampleSubscriptionListeningStatelessService
{
	/// <summary>
	/// The FabricRuntime creates an instance of this class for each service type instance. 
	/// </summary>
	internal sealed class SampleSubscriptionListeningStatelessService : StatelessService
	{
		public SampleSubscriptionListeningStatelessService(StatelessServiceContext serviceContext) : base(serviceContext)
		{
		}

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
				, serviceBusSubscriptionName
                , requireSessions: false), "StatelessService-ServiceBusSubscriptionListener");
		}

	
	}

	internal sealed class Handler : AutoCompleteServiceBusMessageReceiver
	{
		private readonly StatelessService _service;

		public Handler(StatelessService service)
		{
			_service = service;
		}

        protected override Task ReceiveMessageImplAsync(BrokeredMessage message, CancellationToken cancellationToken)
        {
            ServiceEventSource.Current.ServiceMessage(_service, $"Handling subscription message {message.MessageId}");
            return Task.FromResult(true);
        }
    }
}

