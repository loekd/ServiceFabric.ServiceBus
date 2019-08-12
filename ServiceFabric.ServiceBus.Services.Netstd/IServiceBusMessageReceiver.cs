using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;

namespace ServiceFabric.ServiceBus.Services.Netstd
{
	/// <summary>
	/// Marks a class as capable of receiving <see cref="Message"/>s, with added cancellation support.
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
        /// <param name="cancellationToken">When Set, indicates that processing should stop.</param>
        Task ReceiveMessageAsync(Message message, CancellationToken cancellationToken);
    }
}
