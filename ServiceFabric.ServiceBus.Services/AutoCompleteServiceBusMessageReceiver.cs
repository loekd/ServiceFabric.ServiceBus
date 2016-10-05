using System;
using System.Runtime.CompilerServices;
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
        /// Gets or sets a callback for writing logs. (Defaults to null)
        /// </summary>
        public Action<string> LogAction { get; }

        /// <inheritdoc />
        public bool AutoComplete => true;

        /// <summary>
        /// Creates a new instance using the provided log callback.
        /// </summary>
        /// <param name="logAction"></param>
        protected AutoCompleteServiceBusMessageReceiver(Action<string> logAction = null)
        {
            LogAction = logAction;
        }

        /// <summary>
        /// Processes a message. Automatically completes the message after handling, and abandons the lock if an error occurs. 
        /// </summary>
	    /// <param name="messageSession">Contains the MessageSession when sessions are enabled.</param>
        /// <param name="message">The incoming Service Bus Message to process</param>
        /// <param name="cancellationToken">When Set, indicates that processing should stop.</param>
        public async Task ReceiveMessageAsync(BrokeredMessage message, MessageSession messageSession, CancellationToken cancellationToken)
        {
            try
            {
                await ReceiveMessageImplAsync(message, messageSession, cancellationToken);
            }
            catch (Exception ex)
            {
                if (!HandleReceiveMessageError(message, ex))
                    throw;
            }
        }

        /// <summary>
        /// Called when an error is thrown from <see cref="ReceiveMessageAsync"/>. Return true if the error is handled. Return false to terminate the process.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="ex"></param>
        protected virtual bool HandleReceiveMessageError(BrokeredMessage message, Exception ex)
        {
            WriteLog($"Abandoning message {message.MessageId}. Error:'{ex}'.");
            message.Abandon();
            //catch all to avoid process crash.
            //assuming overriding code handles exceptions.
            return true;
        }

        /// <summary>
        /// (When overridden) Processes a message.  Must perform error handling.
        /// </summary>
        /// <param name="message">The incoming Service Bus Message to process</param>
        /// <param name="messageSession">Contains the MessageSession when sessions are enabled.</param>
        /// <param name="cancellationToken">When Set, indicates that processing should stop.</param>
        protected abstract Task ReceiveMessageImplAsync(BrokeredMessage message, MessageSession messageSession, CancellationToken cancellationToken);

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