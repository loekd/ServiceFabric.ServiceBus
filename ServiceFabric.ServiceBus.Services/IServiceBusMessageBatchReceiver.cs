using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace ServiceFabric.ServiceBus.Services
{
    /// <summary>
    /// Marks a class as capable of receiving batched <see cref="BrokeredMessage"/>s, with added cancellation support.
    /// </summary>
    public interface IServiceBusMessageBatchReceiver
    {
        /// <summary>
        /// Indicates whether a batch of messages should be automatically completed after processing.
        /// </summary>
        bool AutoComplete { get; }

        /// <summary>
        /// Processes a message batch. Must perform error handling and also message completion or abandoning.
        /// </summary>
	    /// <param name="messageSession">Contains the MessageSession when sessions are enabled.</param>
        /// <param name="messages">The incoming batch of Service Bus <see cref="BrokeredMessage"/>s to process</param>
        /// <param name="cancellationToken">When Set, indicates that processing should stop.</param>
        Task ReceiveMessagesAsync(IEnumerable<BrokeredMessage> messages, MessageSession messageSession, CancellationToken cancellationToken);
    }
}