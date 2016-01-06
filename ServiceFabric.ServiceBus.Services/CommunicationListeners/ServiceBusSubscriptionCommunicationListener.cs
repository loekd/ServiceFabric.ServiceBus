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
	/// Implementation of <see cref="ICommunicationListener"/> that listens to a Service Bus SubscriptionS.
	/// </summary>
	public class ServiceBusSubscriptionCommunicationListener : ServiceBusCommunicationListener
	{
		private SubscriptionClient _serviceBusClient;

		/// <summary>
		/// Gets the name of the monitored Service Bus Topic.
		/// </summary>
		protected string ServiceBusTopicName { get; }

		/// <summary>
		/// Gets the name of the monitored Service Bus Topic Subscription.
		/// </summary>
		protected string ServiceBusSubscriptionName { get; }

		/// <summary>
		/// Creates a new instance, using the init parameters of a <see cref="StatelessService"/>
		/// </summary>
		/// <param name="receiver">Object that will process incoming messages.</param>
		/// <param name="parameters">The init parameters of a <see cref="StatelessService"/></param>
		/// <param name="serviceBusTopicName">The name of the monitored Service Bus Topic</param>
		/// <param name="serviceBusSubscriptionName">The name of the monitored Service Bus Topic Subscription</param>
		/// <param name="serviceBusSendConnectionString">Optional connection string for sending messages to the queue. If not provided, the configuration file setting 'Microsoft.ServiceBus.ConnectionString.Send' will be used.</param>
		/// <param name="seviceBusReceiveConnectionString">Optional connection string for receiving messages from the queue. If not provided, the configuration file setting 'Microsoft.ServiceBus.ConnectionString.Receive' will be used.</param>
		public ServiceBusSubscriptionCommunicationListener(IServiceBusMessageReceiver receiver, StatelessServiceInitializationParameters parameters, string serviceBusTopicName, string serviceBusSubscriptionName, string serviceBusSendConnectionString = null, string seviceBusReceiveConnectionString = null)
			: base(receiver, parameters, serviceBusSendConnectionString, seviceBusReceiveConnectionString)
		{
			if (string.IsNullOrWhiteSpace(serviceBusTopicName)) throw new ArgumentOutOfRangeException(nameof(serviceBusTopicName));
			if (string.IsNullOrWhiteSpace(serviceBusSubscriptionName)) throw new ArgumentOutOfRangeException(nameof(serviceBusSubscriptionName));

			ServiceBusTopicName = serviceBusTopicName;
			ServiceBusSubscriptionName = serviceBusSubscriptionName;
		}

		/// <summary>
		/// Creates a new instance, using the init parameters of a <see cref="StatefulService"/>
		/// </summary>
		/// <param name="receiver">Object that will process incoming messages.</param>
		/// <param name="parameters">The init parameters of a <see cref="StatefulService"/></param>
		/// <param name="serviceBusTopicName">The name of the monitored Service Bus Topic</param>
		/// <param name="serviceBusSubscriptionName">The name of the monitored Service Bus Topic Subscription</param>
		/// <param name="serviceBusSendConnectionString">Optional connection string for sending messages to the queue. If not provided, the configuration file setting 'Microsoft.ServiceBus.ConnectionString.Send' will be used.</param>
		/// <param name="seviceBusReceiveConnectionString">Optional connection string for receiving messages from the queue. If not provided, the configuration file setting 'Microsoft.ServiceBus.ConnectionString.Receive' will be used.</param>
		public ServiceBusSubscriptionCommunicationListener(IServiceBusMessageReceiver receiver, StatefulServiceInitializationParameters parameters, string serviceBusTopicName, string serviceBusSubscriptionName, string serviceBusSendConnectionString = null, string seviceBusReceiveConnectionString = null)
			: base(receiver, parameters, serviceBusSendConnectionString, seviceBusReceiveConnectionString)
		{
			if (string.IsNullOrWhiteSpace(serviceBusTopicName)) throw new ArgumentOutOfRangeException(nameof(serviceBusTopicName));
			if (string.IsNullOrWhiteSpace(serviceBusSubscriptionName)) throw new ArgumentOutOfRangeException(nameof(serviceBusSubscriptionName));

			ServiceBusTopicName = serviceBusTopicName;
			ServiceBusSubscriptionName = serviceBusSubscriptionName;
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
			_serviceBusClient = SubscriptionClient.CreateFromConnectionString(ServiceBusReceiveConnectionString, ServiceBusTopicName,
				ServiceBusSubscriptionName);

			ListenForMessages(cancellationToken);

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
		/// Starts listening for messages on the configured Service Bus Subscription.
		/// </summary>
		/// <param name="cancellationToken"></param>
		private void ListenForMessages(CancellationToken cancellationToken)
		{
			var options = CreateMessageOptions();

			_serviceBusClient.OnMessage(message =>
			{
				ReceiveMessage(cancellationToken, message);
			}, options);
		}
	}
}