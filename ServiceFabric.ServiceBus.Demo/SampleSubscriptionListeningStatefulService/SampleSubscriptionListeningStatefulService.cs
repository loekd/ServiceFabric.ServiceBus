using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
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

			yield return new ServiceReplicaListener(context => new ServiceBusSubscriptionBatchCommunicationListener(
				new Handler(this)
				, context
				, serviceBusTopicName
				, serviceBusSubscriptionName
                , requireSessions: true)
			{
                MessagePrefetchCount = 10
			}, "StatefulService-ServiceBusSubscriptionListener");
		}

		internal sealed class Handler : AutoCompleteBatchServiceBusMessageReceiver
		{
			private readonly StatefulService _service;

			public Handler(StatefulService service)
			{
				_service = service;
			}

            protected override Task ReceiveMessagesImplAsync(IEnumerable<BrokeredMessage> messages, MessageSession session, CancellationToken cancellationToken)
            {
                var brokeredMessages = messages.ToArray();
                ServiceEventSource.Current.ServiceMessage(_service, $"Handling batch of {brokeredMessages.Count()}  queue messages");

                foreach (var message in brokeredMessages)
                {
                    ServiceEventSource.Current.ServiceMessage(_service, $"Handling queue message {message.MessageId}");
                }
                return Task.FromResult(true);
            }
        }

		
	}
}
