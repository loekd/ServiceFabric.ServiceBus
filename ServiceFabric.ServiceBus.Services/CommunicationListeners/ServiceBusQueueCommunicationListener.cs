using System;
using System.Fabric;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;

namespace ServiceFabric.ServiceBus.Services.CommunicationListeners
{
	/// <summary>
	/// Implementation of <see cref="ICommunicationListener"/> that listens to a Service Bus Queue.
	/// </summary>
	public class ServiceBusQueueCommunicationListener : ServiceBusCommunicationListener
	{
		private QueueClient _serviceBusClient;

		/// <summary>
		/// Gets the name of the monitored Service Bus Queue.
		/// </summary>
		protected string ServiceBusQueueName { get; }

		/// <summary>
		/// Creates a new instance, using the init parameters of a <see cref="StatefulService"/>
		/// </summary>
		/// <param name="receiver">(Required) Processes incoming messages.</param>
		/// <param name="context">(Optional) The context that was used to init the Reliable Service that uses this listener.</param>
		/// <param name="serviceBusQueueName">The name of the monitored Service Bus Queue</param>
		/// <param name="serviceBusSendConnectionString">(Optional) A Service Bus connection string that can be used for Sending messages. 
		/// (Returned as Service Endpoint.) When not supplied, an App.config appSettings value with key 'Microsoft.ServiceBus.ConnectionString.Receive'
		///  is used.</param>
		/// <param name="serviceBusReceiveConnectionString">(Optional) A Service Bus connection string that can be used for Receiving messages. 
		///  When not supplied, an App.config appSettings value with key 'Microsoft.ServiceBus.ConnectionString.Receive'
		///  is used.</param>
		public ServiceBusQueueCommunicationListener(IServiceBusMessageReceiver receiver, ServiceContext context, string serviceBusQueueName, string serviceBusSendConnectionString = null, string serviceBusReceiveConnectionString = null)
			: base(receiver, context, serviceBusSendConnectionString, serviceBusReceiveConnectionString)
		{
			if (string.IsNullOrWhiteSpace(serviceBusQueueName)) throw new ArgumentOutOfRangeException(nameof(serviceBusQueueName));

			ServiceBusQueueName = serviceBusQueueName;
		}

		/// <summary>
		/// This method causes the communication listener to be opened. Once the Open
		///             completes, the communication listener becomes usable - accepts and sends messages.
		/// </summary>
		/// <param name="cancellationToken">Cancellation token</param>
		/// <returns>
		/// A <see cref="T:System.Threading.Tasks.Task">Task</see> that represents outstanding operation. The result of the Task is
		///             the endpoint string.
		/// </returns>
		public override Task<string> OpenAsync(CancellationToken cancellationToken)
		{
			//use receive url:
			_serviceBusClient = QueueClient.CreateFromConnectionString(ServiceBusReceiveConnectionString, ServiceBusQueueName
				, ServiceBusReceiveMode);

			if (ServiceBusMessagePrefetchCount > 0)
			{
				_serviceBusClient.PrefetchCount = ServiceBusMessagePrefetchCount;
			}

			ListenForMessages();
			Thread.Yield();

			//create send url:
			string uri = ServiceBusSendConnectionString;
			return Task.FromResult(uri);
		}


		/// <summary>
		/// This method causes the communication listener to close. Close is a terminal state and 
		///             this method allows the communication listener to transition to this state in a
		///             graceful manner.
		/// </summary>
		/// <param name="cancellationToken">Cancellation token</param>
		/// <returns>
		/// A <see cref="T:System.Threading.Tasks.Task">Task</see> that represents outstanding operation.
		/// </returns>
		protected override async Task CloseImplAsync(CancellationToken cancellationToken)
		{
			await _serviceBusClient.CloseAsync();
		}

		/// <summary>
		/// Starts listening for messages on the configured Service Bus Queue.
		/// </summary>
		private void ListenForMessages()
		{
			ThreadStart ts = () =>
			{
				while (!StopProcessingMessageToken.IsCancellationRequested)
				{
					var messages = _serviceBusClient.ReceiveBatch(ServiceBusMessageBatchSize, ServiceBusServerTimeout);
					foreach (var message in messages)
					{
						if (StopProcessingMessageToken.IsCancellationRequested) break;
						ReceiveMessage(message);
					}
				}
			};
			StartBackgroundThread(ts);
		}
	}
}