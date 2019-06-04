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
    /// Implementation of <see cref="IServiceBusMessageHandler"/> that can automatically call <see cref="IServiceBusCommunicationListener.Complete"/>
    /// on the received message after successful processing.
    /// Upon failure, it will call <see cref="IServiceBusCommunicationListener.Abandon"/> and suppress the error.
    /// Also has dead-letter and cancellation support.
    /// </summary>
    /// <remarks>Use the communication listener constructor that takes 
    /// Func&lt;IServiceBusCommunicationListener, IServiceBusMessageReceiver&gt; receiverFactory</remarks>
    public abstract class DefaultServiceBusMessageHandler : IServiceBusMessageHandler
    {

        /// <summary>
        /// Gets or sets a callback for writing logs. (Defaults to null)
        /// </summary>
        public Action<string> LogAction { get; }

        /// <inheritdoc />
        /// <summary>Defaults to true.</summary>
        public bool AutoComplete { get; set; } = true;

        /// <summary>
        /// Provides access to the provided <see cref="IServiceBusCommunicationListener"/>.
        /// </summary>
        protected IServiceBusCommunicationListener Listener { get; }

        /// <summary>
        /// Creates a new instance using the provided log callback.
        /// </summary>
        /// <param name="receiver"></param>
        /// <param name="logAction"></param>
        protected DefaultServiceBusMessageHandler(IServiceBusCommunicationListener receiver, Action<string> logAction)
        {
            Listener = receiver ?? throw new ArgumentNullException(nameof(receiver));
            LogAction = logAction;
        }
        /// <summary>
        /// Creates a new instance using the provided log callback.
        /// </summary>
        /// <param name="receiver"></param>
        protected DefaultServiceBusMessageHandler(IServiceBusCommunicationListener receiver)
        {
            Listener = receiver ?? throw new ArgumentNullException(nameof(receiver));
        }

        /// <summary>
        /// Processes a message. Automatically completes the message after handling if <see cref="AutoComplete"/> is true, 
        /// and abandons the lock if an error occurs. 
        /// </summary>
        /// <param name="message">The incoming Service Bus Message to process</param>
        /// <param name="cancellationToken">When Set, indicates that processing should stop.</param>
        public async Task HandleAsync(Message message, CancellationToken cancellationToken)
        {
            try
            {
                await ReceiveMessageImplAsync(message, cancellationToken).ConfigureAwait(false);
                if (AutoComplete)
                {
                    await CompleteMessage(message).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                if (!await HandleReceiveMessageError(message, ex).ConfigureAwait(false))
                    throw;
            }
        }

        /// <summary>
        /// Called when an error is thrown from <see cref="HandleAsync"/>.
        /// Logs the error using <see cref="LogAction"/> and calls <see cref="AbandonMessage"/>. 
        /// When overridden: Return true if the error is handled. Return false to terminate the process.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="ex"></param>
        protected virtual async Task<bool> HandleReceiveMessageError(Message message, Exception ex)
        {
            WriteLog($"Abandoning message {message.MessageId}. Error:'{ex}'.");
            await Listener.Abandon(message).ConfigureAwait(false);
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
            return Listener.Abandon(message, propertiesToModify);
        }

        /// <summary>
        /// Moves the provided message to the dead letter queue.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="propertiesToModify"></param>
        protected Task DeadLetterMessage(Message message, IDictionary<string, object> propertiesToModify)
        {
            WriteLog($"Moving message {message.MessageId} to dead letter queue.");
            return Listener.DeadLetter(message, propertiesToModify);
        }

        /// <summary>
        /// Completes the provided message.
        /// </summary>
        /// <param name="message"></param>
        protected Task CompleteMessage(Message message)
        {
            WriteLog($"Completing message {message.MessageId}.");
            return Listener.Complete(message);
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
