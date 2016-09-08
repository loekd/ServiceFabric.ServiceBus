using System;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using System.Collections.Generic;
using System.Fabric;
using System.Threading;
using Microsoft.Azure;
using Microsoft.ServiceBus.Messaging;
using ServiceFabric.ServiceBus.Services;
using ServiceFabric.ServiceBus.Services.CommunicationListeners;
using System.Threading.Tasks;

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
		    Action<string> logAction = log => ServiceEventSource.Current.ServiceMessage(this, log);
		    yield return new ServiceInstanceListener(context => new ServiceBusQueueCommunicationListener(
				new Handler(logAction)
				, context
				, serviceBusQueueName
                , requireSessions: false)
			{
			    MessageLockRenewTimeSpan = TimeSpan.FromSeconds(50),  //auto renew every 50s, so processing can take longer than 60s (default lock duration).
                LogAction = logAction
			}, "StatelessService-ServiceBusQueueListener");
		}

		
	}

	internal sealed class Handler : AutoCompleteServiceBusMessageReceiver
	{

		public Handler(Action<string> logAction)
            :base(logAction)
		{
		}

		
        protected override Task ReceiveMessageImplAsync(BrokeredMessage message, CancellationToken cancellationToken)
        {
            WriteLog($"Sleeping for 7s while processing queue message {message.MessageId} to test message lock renew function (send more than 9 messages!).");
            Thread.Sleep(TimeSpan.FromSeconds(7));

            WriteLog($"Handling queue message {message.MessageId}");
            return Task.FromResult(true);
        }
    }
}
