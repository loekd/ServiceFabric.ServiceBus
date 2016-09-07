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
using System.Threading.Tasks;

namespace SampleSubscriptionListeningStatefulService
{
	/// <summary>
	/// The FabricRuntime creates an instance of this class for each service type instance.
	/// </summary>
	internal sealed class SampleSubscriptionListeningStatefulService : StatefulService
	{
		public SampleSubscriptionListeningStatefulService(StatefulServiceContext serviceContext) : base(serviceContext)
		{
		}

		public SampleSubscriptionListeningStatefulService(StatefulServiceContext serviceContext, IReliableStateManagerReplica reliableStateManagerReplica) : base(serviceContext, reliableStateManagerReplica)
		{
		}

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
				, serviceBusSubscriptionName
                , requireSessions: true), "StatefulService-ServiceBusSubscriptionListener");
		}

		internal sealed class Handler : AutoCompleteServiceBusMessageReceiver
		{
			private readonly StatefulService _service;

			public Handler(StatefulService service)
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
}
