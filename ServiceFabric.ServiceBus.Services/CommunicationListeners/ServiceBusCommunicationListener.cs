using System;
using System.Fabric;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure;
using Microsoft.ServiceBus.Messaging;
using Microsoft.ServiceFabric.Services.Communication.Runtime;

namespace ServiceFabric.ServiceBus.Services.CommunicationListeners
{
	/// <summary>
	/// Abstract base implementation for <see cref="ICommunicationListener"/> connected to ServiceBus
	/// </summary>
	public abstract class ServiceBusCommunicationListener : ICommunicationListener, IDisposable
	{
		private const string DefaultSendConnectionStringConfigurationKey = "Microsoft.ServiceBus.ConnectionString.Send";
		private const string DefaultReceiveConnectionStringConfigurationKey = "Microsoft.ServiceBus.ConnectionString.Receive";

		//prevents aborts during the processing of a message
		private readonly ManualResetEvent _processingMessage = new ManualResetEvent(true);

		/// <summary>
		/// Gets the processor of incoming messages
		/// </summary>
		protected IServiceBusMessageReceiver Receiver { get; }

		/// <summary>
		/// Gets the ServiceInitializationParameters that were used to create this instance.
		/// </summary>
		protected ServiceInitializationParameters Parameters { get; }
		
		/// <summary>
		/// Gets a Service Bus connection string that should have only receive-rights.
		/// </summary>
		protected string ServiceBusReceiveConnectionString { get; }

		/// <summary>
		/// Gets a Service Bus connection string that should have only send-rights.
		/// </summary>
		protected string ServiceBusSendConnectionString { get; }

		/// <summary>
		/// Gets the end of the service EndPoint uri that is returned to the caller.
		/// </summary>
		protected string LastUriSegment { get; }

		protected ServiceBusCommunicationListener(IServiceBusMessageReceiver receiver
			, StatelessServiceInitializationParameters parameters
			, string serviceBusSendConnectionString
			, string seviceBusReceiveConnectionString)
			: this(receiver, serviceBusSendConnectionString, seviceBusReceiveConnectionString)
		{
			if (parameters == null) throw new ArgumentNullException(nameof(parameters));
			LastUriSegment = $"{parameters.PartitionId}-{parameters.InstanceId.ToString(CultureInfo.InvariantCulture)}";
			Parameters = parameters;
		}

		protected ServiceBusCommunicationListener(IServiceBusMessageReceiver receiver
			, StatefulServiceInitializationParameters parameters
			, string serviceBusSendConnectionString
			, string seviceBusReceiveConnectionString)
			: this(receiver, serviceBusSendConnectionString, seviceBusReceiveConnectionString)
		{
			if (parameters == null) throw new ArgumentNullException(nameof(parameters));
			LastUriSegment = $"{parameters.PartitionId}-{parameters.ReplicaId.ToString(CultureInfo.InvariantCulture)}";
			Parameters = parameters;
		}

		private ServiceBusCommunicationListener(IServiceBusMessageReceiver receiver
			, string serviceBusSendConnectionString
			, string serviceBusReceiveConnectionString)
		{
			if (receiver == null) throw new ArgumentNullException(nameof(receiver));

			if (string.IsNullOrWhiteSpace(serviceBusSendConnectionString))
				serviceBusSendConnectionString = CloudConfigurationManager.GetSetting(DefaultSendConnectionStringConfigurationKey);
			if (string.IsNullOrWhiteSpace(serviceBusReceiveConnectionString))
				serviceBusReceiveConnectionString = CloudConfigurationManager.GetSetting(DefaultReceiveConnectionStringConfigurationKey);

			if (string.IsNullOrWhiteSpace(serviceBusSendConnectionString)) throw new ArgumentOutOfRangeException(nameof(serviceBusSendConnectionString));
			if (string.IsNullOrWhiteSpace(serviceBusReceiveConnectionString)) throw new ArgumentOutOfRangeException(nameof(serviceBusReceiveConnectionString));

			Receiver = receiver;
			ServiceBusSendConnectionString = serviceBusSendConnectionString;
			ServiceBusReceiveConnectionString = serviceBusReceiveConnectionString;
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
		public abstract Task<string> OpenAsync(CancellationToken cancellationToken);

		/// <summary>
		/// This method causes the communication listener to close. Close is a terminal state and 
		///             this method allows the communication listener to transition to this state in a
		///             graceful manner.
		/// </summary>
		/// <param name="cancellationToken">Cancellation token</param>
		/// <returns>
		/// A <see cref="T:System.Threading.Tasks.Task">Task</see> that represents outstanding operation.
		/// </returns>
		public Task CloseAsync(CancellationToken cancellationToken)
		{
			_processingMessage.WaitOne();
			_processingMessage.Dispose();
			return CloseImplAsync(cancellationToken);
		}

		/// <summary>
		/// This method causes the communication listener to close. Close is a terminal state and
		///             this method causes the transition to close ungracefully. Any outstanding operations
		///             (including close) should be canceled when this method is called.
		/// </summary>
		public virtual void Abort()
		{
			_processingMessage.Dispose();
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
		protected virtual Task CloseImplAsync(CancellationToken cancellationToken)
		{
			return Task.FromResult(true);
		}

		/// <summary>
		/// Will pass an incoming message to the <see cref="Receiver"/> for processing.
		/// </summary>
		/// <param name="cancellationToken"></param>
		/// <param name="message"></param>
		protected void ReceiveMessage(CancellationToken cancellationToken, BrokeredMessage message)
		{
			try
			{
				_processingMessage.Reset();
				if (cancellationToken.IsCancellationRequested)
				{
					throw new OperationCanceledException("Cancellation requested.");
				}

				Receiver.ReceiveMessage(message);
				message.Complete();
			}
			catch
			{
				message.Abandon();
			}
			finally
			{
				_processingMessage.Set();
			}
		}

		/// <summary>
		/// Configures the way messages are received from Service Bus.
		/// </summary>
		/// <returns></returns>
		protected virtual OnMessageOptions CreateMessageOptions()
		{
			var options = new OnMessageOptions
			{
				AutoComplete = false,
				AutoRenewTimeout = TimeSpan.FromMinutes(1),
			};
			return options;
		}

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		public void Dispose()
		{
			Dispose(true);
		}

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		protected virtual void Dispose(bool disposing)
		{
			if (disposing)
			{
				Abort();
			}
		}
	}
}
