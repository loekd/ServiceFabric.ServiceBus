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
        public async Task ReceiveMessageAsync(BrokeredMessage message, CancellationToken cancellationToken)
        {
            try
            {
                await ReceiveMessageImplAsync(message, cancellationToken);
                message.Complete();
            }
            catch
            {
                message.Abandon();
                //catch all to avoid process crash.
                //assuming implementing code handles exceptions.
            }
            finally
            {
                message.Dispose();
            }
        }

        /// <summary>
        /// (When overridden) Processes a message.  Must perform error handling.
        /// </summary>
        /// <param name="message">The incoming Service Bus Message to process</param>
        /// <param name="cancellationToken">When Set, indicates that processing should stop.</param>
        protected abstract Task ReceiveMessageImplAsync(BrokeredMessage message, CancellationToken cancellationToken);
    }
}