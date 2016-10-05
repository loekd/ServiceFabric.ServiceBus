using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace ServiceFabric.ServiceBus.Services
{
    /// <summary>
    /// Implementation of <see cref="IServiceBusMessageBatchReceiver"/> that will automatically call Batch Complete"/>
    /// on the received message after successfull processing.
    /// Upon failure, it will call <see cref="BrokeredMessage.Abandon()"/> and suppress the error.
    /// Also has cancellation support.
    /// </summary>
    public abstract class AutoCompleteBatchServiceBusMessageReceiver : IServiceBusMessageBatchReceiver
    {
        /// <summary>
        /// Gets or sets a callback for writing logs. (Defaults to null)
        /// </summary>
        public Action<string> LogAction { get; }
        
        /// <inheritdoc />
        public bool AutoComplete => true;

        /// <summary>
        /// Creates a new instance using the provided log callback.
        /// </summary>
        /// <param name="logAction"></param>
        protected AutoCompleteBatchServiceBusMessageReceiver(Action<string> logAction = null)
        {
            LogAction = logAction;
        }

        /// <summary>
        /// Processes a message. Automatically completes the message after handling, and abandons the lock if an error occurs. 
        /// </summary>
        /// <param name="messageSession">Contains the MessageSession when sessions are enabled.</param>
        /// <param name="messages">The incoming Service Bus Message batch to process</param>
        /// <param name="cancellationToken">When Set, indicates that processing should stop.</param>
        public async Task ReceiveMessagesAsync(IEnumerable<BrokeredMessage> messages, MessageSession messageSession, CancellationToken cancellationToken)
        {
            var localMessages = messages.ToArray();
            try
            {
                await ReceiveMessagesImplAsync(localMessages, messageSession, cancellationToken);
            }
            catch (Exception ex)
            {
                if (!HandleReceiveMessagesError(localMessages, ex))
                    throw;
            }
        }

        /// <summary>
        /// Called when an error is thrown from <see cref="ReceiveMessagesAsync"/>. Return true if the error is handled. Return false to terminate the process.
        /// Abandons all messages in the batch. Returns true.
        /// </summary>
        /// <param name="messages"></param>
        /// <param name="ex"></param>
        protected virtual bool HandleReceiveMessagesError(IEnumerable<BrokeredMessage> messages, Exception ex)
        {
            foreach (var message in messages)
            {
                WriteLog($"Abandoning message {message.MessageId}. Error:'{ex}'.");
                message.Abandon();
            }
            //catch all to avoid process crash.
            //assuming overriding code handles exceptions.
            return true;
        }

        /// <summary>
        /// (When overridden) Processes a message.  Must perform error handling.
        /// </summary>
        /// <param name="messages">The incoming Service Bus Message batch to process</param>
        /// <param name="messageSession">Contains the MessageSession when sessions are enabled.</param>
        /// <param name="cancellationToken">When Set, indicates that processing should stop.</param>
        protected abstract Task ReceiveMessagesImplAsync(IEnumerable<BrokeredMessage> messages, MessageSession messageSession, CancellationToken cancellationToken);

        /// <summary>
        /// Writes a log entry if <see cref="LogAction"/> is not null.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="callerMemberName"></param>
        protected void WriteLog(string message, [CallerMemberName]string callerMemberName = "unknown")
        {
            LogAction?.Invoke($"{GetType().FullName} \t {callerMemberName} \t {message}");
        }
    }
}