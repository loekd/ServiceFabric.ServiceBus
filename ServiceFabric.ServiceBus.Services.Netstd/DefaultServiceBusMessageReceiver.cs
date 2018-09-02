using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using ServiceFabric.ServiceBus.Services.Netstd.CommunicationListeners;

namespace ServiceFabric.ServiceBus.Services.Netstd
{
    /// <summary>
    /// Implementation of <see cref="IServiceBusMessageReceiver"/> that can automatically call <see cref="IServiceBusCommunicationListener.Complete"/>
    /// on the received message after successfull processing.
    /// Upon failure, it will call <see cref="IServiceBusCommunicationListener.Abandon"/> and suppress the error.
    /// Also has dead-letter and cancellation support.
    /// </summary>
    /// <remarks>Use the communication listener constructor that takes 
    /// Func&lt;IServiceBusCommunicationListener, IServiceBusMessageReceiver&gt; receiverFactory</remarks>
    public abstract class DefaultServiceBusMessageReceiver : IServiceBusMessageReceiver
    {
        private readonly IServiceBusCommunicationListener _receiver;

        /// <summary>
        /// Gets or sets a callback for writing logs. (Defaults to null)
        /// </summary>
        public Action<string> LogAction { get; }

        /// <inheritdoc />
        /// <summary>Defaults to true.</summary>
        public bool AutoComplete { get; set; } = true;
    
        /// <summary>
        /// Creates a new instance using the provided log callback.
        /// </summary>
        /// <param name="receiver"></param>
        /// <param name="logAction"></param>
        protected DefaultServiceBusMessageReceiver(IServiceBusCommunicationListener receiver, Action<string> logAction)
        {
            _receiver = receiver ?? throw new ArgumentNullException(nameof(receiver));
            LogAction = logAction;
        }
        /// <summary>
        /// Creates a new instance using the provided log callback.
        /// </summary>
        /// <param name="receiver"></param>
        protected DefaultServiceBusMessageReceiver(IServiceBusCommunicationListener receiver)
        {
            _receiver = receiver ?? throw new ArgumentNullException(nameof(receiver));
        }

        /// <summary>
        /// Processes a message. Automatically completes the message after handling if <see cref="AutoComplete"/> is true, 
        /// and abandons the lock if an error occurs. 
        /// </summary>
        /// <param name="message">The incoming Service Bus Message to process</param>
        /// <param name="cancellationToken">When Set, indicates that processing should stop.</param>
        public async Task ReceiveMessageAsync(Message message, CancellationToken cancellationToken)
        {
            try
            {
                await ReceiveMessageImplAsync(message, cancellationToken);
                if (AutoComplete)
                {
                    await _receiver.Complete(message);
                }
            }
            catch (Exception ex)
            {
                if (!await HandleReceiveMessageError(message, ex).ConfigureAwait(false))
                    throw;
            }
        }

        /// <summary>
        /// Called when an error is thrown from <see cref="ReceiveMessageAsync"/>.
        /// Logs the error using <see cref="LogAction"/> and calls <see cref="AbandonMessage"/>. 
        /// When overridden: Return true if the error is handled. Return false to terminate the process.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="ex"></param>
        protected virtual async Task<bool> HandleReceiveMessageError(Message message, Exception ex)
        {
            WriteLog($"Abandoning message {message.MessageId}. Error:'{ex}'.");
            await _receiver.Abandon(message).ConfigureAwait(false);
            //assuming overriding code handles exceptions.
            return true;
        }

        /// <summary>
        /// Removes the lock on the provided message to put it back on the queue for later consumption.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="propertiesToModify"></param>
        protected Task AbandonMessage(Message message, IDictionary<string, object> propertiesToModify)
        {
            WriteLog($"Moving message {message.MessageId} to dead letter queue.");
            return _receiver.Abandon(message, propertiesToModify);
        }

        /// <summary>
        /// Moves the provided message to the dead letter queue.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="propertiesToModify"></param>
        protected Task DeadLetterMessage(Message message, IDictionary<string, object> propertiesToModify)
        {
            WriteLog($"Moving message {message.MessageId} to dead letter queue.");
            return _receiver.DeadLetter(message, propertiesToModify);
        }

        /// <summary>
        /// (When overridden) Processes a message.  Must perform error handling.
        /// </summary>
        /// <param name="message">The incoming Service Bus Message to process</param>
        /// <param name="cancellationToken">When Set, indicates that processing should stop.</param>
        protected abstract Task ReceiveMessageImplAsync(Message message, CancellationToken cancellationToken);

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
