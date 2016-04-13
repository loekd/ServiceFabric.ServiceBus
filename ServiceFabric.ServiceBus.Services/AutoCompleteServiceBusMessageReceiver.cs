using System.Threading;
using Microsoft.ServiceBus.Messaging;

namespace ServiceFabric.ServiceBus.Services
{
	/// <summary>
	/// Implementation of <see cref="IServiceBusMessageReceiver"/> that will automatically call <see cref="BrokeredMessage.Complete()"/>
	/// on the received message after successfull processing.
	/// Upon failure, it will call <see cref="BrokeredMessage.Abandon()"/>.
	/// Also has cancellation support.
	/// </summary>
	public abstract class AutoCompleteServiceBusMessageReceiver : ICancelableServiceBusMessageReceiver
	{
		/// <summary>
		/// Processes a message. Must perform error handling and also message completion or abandoning.
		/// </summary>
		/// <param name="message">The incoming Service Bus Message to process</param>
		/// <param name="cancellationToken">When Set, indicates that processing should stop.</param>
		public void ReceiveMessage(BrokeredMessage message, CancellationToken cancellationToken)
		{
			try
			{
				ReceiveMessageImpl(message, cancellationToken);
				message.Complete();
			}
			catch
			{
				message.Abandon();
			}
		}

		/// <summary>
		/// Processes a message. Must perform error handling and also message completion or abandoning.
		/// </summary>
		/// <param name="message">The incoming Service Bus Message to process</param>
		public void ReceiveMessage(BrokeredMessage message)
		{
			this.ReceiveMessage(message, CancellationToken.None);
		}

		/// <summary>
		/// (When overridden) Processes a message.
		/// </summary>
		/// <param name="message">The incoming Service Bus Message to process</param>
		/// <param name="cancellationToken">When Set, indicates that processing should stop.</param>
		protected abstract void ReceiveMessageImpl(BrokeredMessage message, CancellationToken cancellationToken);


	}
}