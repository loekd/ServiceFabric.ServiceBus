using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace ServiceFabric.ServiceBus.Services
{
    /// <summary>
    /// Implementation of <see cref="IServiceBusMessageReceiver"/> that will automatically call <see cref="BrokeredMessage.Complete()"/>
    /// on the received message after successfull processing.
    /// Upon failure, it will call <see cref="BrokeredMessage.Abandon()"/> and suppress the error.
    /// Also has cancellation support.
    /// </summary>
    public abstract class AutoCompleteServiceBusMessageReceiver : IServiceBusMessageReceiver
    {
        /// <summary>
        /// Processes a message. Must perform error handling and also message completion or abandoning.
        /// </summary>
        /// <param name="message">The incoming Service Bus Message to process</param>
        /// <param name="cancellationToken">When Set, indicates that processing should stop.</param>
        public void ReceiveMessage(BrokeredMessage message, CancellationToken cancellationToken)
        {
            ReceiveMessageImplAsync(message, cancellationToken).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Processes a message. Must perform error handling and also message completion or abandoning.
        /// </summary>
        /// <param name="message">The incoming Service Bus Message to process</param>
        public void ReceiveMessage(BrokeredMessage message)
        {
            ReceiveMessage(message, CancellationToken.None);
        }

        // <summary>
        /// Processes a message. Must perform error handling and also message completion or abandoning.
        /// </summary>
        /// <param name="message">The incoming Service Bus Message to process</param>
        /// <param name="cancellationToken">When Set, indicates that processing should stop.</param>
        public Task ReceiveMessageAsync(BrokeredMessage message, CancellationToken cancellationToken)
        {
            Task result = null;
            try
            {
                result = ReceiveMessageImplAsync(message, cancellationToken);
                message.Complete();
            }
            catch
            {
                message.Abandon();
                //catch all to avoid process crash.
                //assuming implementing code handles exceptions.
            }
            return result;
        }

        /// <summary>
		/// Processes a message. Must perform error handling and also message completion or abandoning.
		/// </summary>
		/// <param name="message">The incoming Service Bus Message to process</param>
        public Task ReceiveMessageAsync(BrokeredMessage message)
        {
            return ReceiveMessageAsync(message, CancellationToken.None);
        }

        /// <summary>
        /// (When overridden) Processes a message.
        /// </summary>
        /// <param name="message">The incoming Service Bus Message to process</param>
        /// <param name="cancellationToken">When Set, indicates that processing should stop.</param>
        protected abstract Task ReceiveMessageImplAsync(BrokeredMessage message, CancellationToken cancellationToken);
    }
}