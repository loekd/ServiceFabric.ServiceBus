using System.Threading;
using Microsoft.ServiceBus.Messaging;
using System.Threading.Tasks;

namespace ServiceFabric.ServiceBus.Services
{
	/// <summary>
	/// Marks a class as capable of receiving <see cref="BrokeredMessage"/>s, with added cancellation support.
	/// </summary>
	public interface IServiceBusMessageReceiver
	{
        /// <summary>
        /// Indicates whether a batch of messages should be automatically completed after processing.
        /// </summary>
        bool AutoComplete { get; }

        /// <summary>
        /// Processes a message. Must perform error handling and also message completion or abandoning.
        /// </summary>
        /// <param name="message">The incoming Service Bus Message to process</param>
        /// <param name="messageSession">Contains the MessageSession when sessions are enabled.</param>
        /// <param name="cancellationToken">When Set, indicates that processing should stop.</param>
        Task ReceiveMessageAsync(BrokeredMessage message, MessageSession messageSession, CancellationToken cancellationToken);
    }
}
