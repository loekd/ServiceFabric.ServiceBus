using System.Threading;
using Microsoft.ServiceBus.Messaging;

namespace ServiceFabric.ServiceBus.Services
{
	/// <summary>
	/// Marks a class as capable of receiving <see cref="BrokeredMessage"/>s.
	/// </summary>
	public interface IServiceBusMessageReceiver
	{
		/// <summary>
		/// Processes a message. Must perform error handling and also message completion or abandoning.
		/// </summary>
		/// <param name="message">The incoming Service Bus Message to process</param>
		void ReceiveMessage(BrokeredMessage message);
	}

	/// <summary>
	/// Marks a class as capable of receiving <see cref="BrokeredMessage"/>s, with added cancellation support. (To keep backwards compatibility)
	/// </summary>
	public interface ICancelableServiceBusMessageReceiver : IServiceBusMessageReceiver
	{
		/// <summary>
		/// Processes a message. Must perform error handling and also message completion or abandoning.
		/// </summary>
		/// <param name="message">The incoming Service Bus Message to process</param>
		/// <param name="cancellationToken">When Set, indicates that processing should stop.</param>
		void ReceiveMessage(BrokeredMessage message, CancellationToken cancellationToken);
	}
}
