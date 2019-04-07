using System;
using System.Collections.Generic;
using System.Fabric;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.ServiceFabric.Services.Communication.Runtime;

namespace ServiceFabric.ServiceBus.Services.Netstd.CommunicationListeners
{
    /// <summary>
    /// Communication listener that receives messages from Service Bus.
    /// </summary>
    public interface IServiceBusCommunicationListener : ICommunicationListener, IDisposable
    {
        /// <summary>
        /// Completes the provided message. Removes it from the queue.
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        Task Complete(Message message);

        /// <summary>
        /// Abandons the lock on the provided message. Puts it back on the queue.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="propertiesToModify"></param>
        /// <returns></returns>
        Task Abandon(Message message, IDictionary<string, object> propertiesToModify = null);

        /// <summary>
        /// Moves the provided message to the dead letter queue. Removes it from the queue.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="propertiesToModify"></param>
        /// <returns></returns>
        Task DeadLetter(Message message, IDictionary<string, object> propertiesToModify = null);
    }

    /// <summary>
    /// Abstract base implementation for <see cref="ICommunicationListener"/> connected to ServiceBus
    /// </summary>
    public abstract class ServiceBusCommunicationListener : IServiceBusCommunicationListener
    {
        private readonly CancellationTokenSource _stopProcessingMessageTokenSource;

        protected int ConcurrencyCount = 1;

        protected bool IsClosing;

        //prevents aborts during the processing of a message
        protected SemaphoreSlim ProcessingMessage { get; set; }

        /// <summary>
        /// Gets the <see cref="ServiceContext"/> that was used to create this instance. Can be null.
        /// </summary>
        protected ServiceContext Context { get; }

        /// <summary>
        /// Gets a Service Bus connection string that should have only receive-rights.
        /// </summary>
        protected string ReceiveConnectionString { get; }

        /// <summary>
        /// Gets a Service Bus connection string that should have only send-rights.
        /// </summary>
        protected string SendConnectionString { get; }

        /// <summary>
        /// When <see cref="CancellationToken.IsCancellationRequested"/> is true, this indicates that either <see cref="CloseAsync"/> 
        /// or <see cref="Abort"/> was called.
        /// </summary>
        protected CancellationToken StopProcessingMessageToken { get; }

        /// <summary>
        /// Gets or sets the amount of time to wait for remaining messages to process when <see cref="CloseAsync(CancellationToken)"/> is invoked.
        /// Defaults to 1 minute.
        /// </summary>
        public TimeSpan CloseTimeout { get; set; } = TimeSpan.FromMinutes(1);

        /// <summary>
        /// Gets or sets the prefetch size when receiving Service Bus Messages. (Defaults to 0, which indicates no prefetch)
        /// Set to 20 times the total number of messages that a single receiver can process per second.
        /// </summary>
        public int MessagePrefetchCount { get; set; }

        /// <summary>
        /// Gets or sets the timeout for receiving a batch of Service Bus Messages. (Defaults to 30s)
        /// </summary>
        public TimeSpan ServerTimeout { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Gets or sets the Service Bus client ReceiveMode. 
        /// </summary>
        public ReceiveMode ReceiveMode { get; set; } = ReceiveMode.PeekLock;

        /// <summary>
        /// Gets or sets a callback for writing logs. (Defaults to null)
        /// </summary>
        public Action<string> LogAction { get; set; }

        /// <summary>
        /// Retry policy for client.
        /// </summary>
        public RetryPolicy RetryPolicy { get; set; }


        /// <summary>
        /// Creates a new instance.
        /// </summary>
        /// <param name="context">(Optional) The context that was used to init the Reliable Service that uses this listener.</param>
        /// <param name="serviceBusSendConnectionString">(Optional) A Service Bus connection string that can be used for Sending messages. 
        /// (Returned as Service Endpoint.).
        /// </param>
        /// <param name="serviceBusReceiveConnectionString">(Required) A Service Bus connection string that can be used for Receiving messages. 
        /// </param>
        protected ServiceBusCommunicationListener(ServiceContext context
            , string serviceBusSendConnectionString
            , string serviceBusReceiveConnectionString
            )
        {
            if (string.IsNullOrWhiteSpace(serviceBusSendConnectionString)) serviceBusSendConnectionString = "not:/available";
            if (string.IsNullOrWhiteSpace(serviceBusReceiveConnectionString)) throw new ArgumentOutOfRangeException(nameof(serviceBusReceiveConnectionString));

            Context = context;
            SendConnectionString = serviceBusSendConnectionString;
            ReceiveConnectionString = serviceBusReceiveConnectionString;

            _stopProcessingMessageTokenSource = new CancellationTokenSource();
            StopProcessingMessageToken = _stopProcessingMessageTokenSource.Token;
        }

        /// <summary>
        /// This method causes the communication listener to be opened. Once the Open
        /// completes, the communication listener becomes usable - accepts and sends messages.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>
        /// A <see cref="T:System.Threading.Tasks.Task">Task</see> that represents outstanding operation. The result of the Task is
        /// the endpoint string.
        /// </returns>
        public abstract Task<string> OpenAsync(CancellationToken cancellationToken);

        /// <summary>
        /// This method causes the communication listener to close. Close is a terminal state and 
        /// this method allows the communication listener to transition to this state in a
        /// graceful manner.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>
        /// A <see cref="T:System.Threading.Tasks.Task">Task</see> that represents outstanding operation.
        /// </returns>
        public Task CloseAsync(CancellationToken cancellationToken)
        {
            WriteLog("Service Bus Communication Listnener closing");
            IsClosing = true;
            _stopProcessingMessageTokenSource.Cancel();

            //Wait for Message processing to complete..
            Task.WaitAny(
                // Timeout task.
                Task.Run(() => Thread.Sleep(CloseTimeout)),
                // Wait for all processing messages to finish.
                Task.Run(() =>
                {
                    while(ConcurrencyCount > 0)
                    {
                        ProcessingMessage.Wait();
                        ConcurrencyCount--;
                    }
                }));

            ProcessingMessage.Dispose();
            return CloseImplAsync(cancellationToken);
        }

        /// <summary>
        /// This method causes the communication listener to close. Close is a terminal state and
        /// this method causes the transition to close ungracefully. Any outstanding operations
        /// (including close) should be canceled when this method is called.
        /// </summary>
        public virtual void Abort()
        {
            WriteLog("Service Bus Communication Listnener aborting");
            Dispose();
        }

        /// <summary>
        /// This method causes the communication listener to close. Close is a terminal state and 
        ///             this method allows the communication listener to transition to this state in a
        ///             graceful manner.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>
        /// A <see cref="T:System.Threading.Tasks.Task">Task</see> that represents outstanding operation.
        /// </returns>
        protected virtual Task CloseImplAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(true);
        }


        /// <summary>
        /// Writes a log entry if <see cref="LogAction"/> is not null.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="callerMemberName"></param>
        protected void WriteLog(string message, [CallerMemberName]string callerMemberName = "unknown")
        {
            LogAction?.Invoke($"{GetType().FullName} \t {callerMemberName} \t {message}");
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            GC.SuppressFinalize(this);
            Dispose(true);
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        protected virtual void Dispose(bool disposing)
        {
            if (!disposing) return;

            _stopProcessingMessageTokenSource.Cancel();
            _stopProcessingMessageTokenSource.Dispose();
        }

        /// <inheritdoc />
        public abstract Task Complete(Message message);

        /// <inheritdoc />
        public abstract Task Abandon(Message message, IDictionary<string, object> propertiesToModify = null);

        /// <inheritdoc />
        public abstract Task DeadLetter(Message message, IDictionary<string, object> propertiesToModify = null);
    }
}
