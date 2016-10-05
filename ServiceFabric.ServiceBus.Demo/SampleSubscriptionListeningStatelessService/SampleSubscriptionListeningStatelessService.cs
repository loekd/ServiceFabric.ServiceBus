using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
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
		    string serviceBusTopicName = null; //CloudConfigurationManager.GetSetting("TopicName");
			string serviceBusSubscriptionName = CloudConfigurationManager.GetSetting("SubscriptionName");
            Action<string> logAction = log => ServiceEventSource.Current.ServiceMessage(this, log);

            yield return new ServiceInstanceListener(context => new ServiceBusSubscriptionBatchCommunicationListener(
				new Handler(logAction)
				, context
				, serviceBusTopicName
				, serviceBusSubscriptionName
                , requireSessions: false)
			{
                LogAction = log => ServiceEventSource.Current.ServiceMessage(this, log),
                MessageLockRenewTimeSpan = TimeSpan.FromSeconds(50),  //auto renew every 50s, so processing can take longer than 60s (default lock duration).
                
            }, "StatelessService-ServiceBusSubscriptionListener");
		}
	}

	internal sealed class Handler : AutoCompleteBatchServiceBusMessageReceiver
    {
        public Handler(Action<string> logAction)
            : base(logAction)
        {
        }

        protected override Task ReceiveMessagesImplAsync(IEnumerable<BrokeredMessage> messages, MessageSession session, CancellationToken cancellationToken)
        {
            var brokeredMessages = messages.ToArray();
            WriteLog($"Handling batch of {brokeredMessages.Count()}  queue messages");

            foreach (var message in brokeredMessages)
            {
                WriteLog($"Sleeping for 7s while processing queue message {message.MessageId} to test message lock renew function (send more than 9 messages!).");
                Thread.Sleep(TimeSpan.FromSeconds(7));

                WriteLog($"Handling queue message {message.MessageId}");
            }
            return Task.FromResult(true);
        }

	    protected override bool HandleReceiveMessagesError(IEnumerable<BrokeredMessage> messages, Exception ex)
	    {
            WriteLog($"Handling Receive Messages Error");
            return true;
	    }
	}
}

