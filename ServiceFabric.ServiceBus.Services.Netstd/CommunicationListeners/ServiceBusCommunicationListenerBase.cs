﻿using Microsoft.Azure.ServiceBus;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using System;
using System.Collections.Generic;
using System.Fabric;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceFabric.ServiceBus.Services.Netstd.CommunicationListeners
{
   
    /// <summary>
    /// Abstract base implementation for <see cref="ICommunicationListener"/> connected to ServiceBus
    /// </summary>
    public abstract class ServiceBusCommunicationListenerBase : IServiceBusCommunicationListener
    {
        private readonly CancellationTokenSource _stopProcessingMessageTokenSource;

        //prevents aborts during the processing of a message
        protected SemaphoreSlim ProcessingMessage { get; private set; }

        /// <summary>
        /// Gets the <see cref="ServiceContext"/> that was used to create this instance. Can be null.
        /// </summary>
        protected ServiceContext Context { get; }

        /// <summary>
        /// When <see cref="CancellationToken.IsCancellationRequested"/> is true, this indicates that either <see cref="CloseAsync"/> 
        /// or <see cref="Abort"/> was called.
        /// </summary>
        protected CancellationToken StopProcessingMessageToken { get; }

        /// <summary>
        /// Gets or sets the amount of time to wait for remaining messages to process when <see cref="CloseAsync(CancellationToken)"/> is invoked.
        /// Defaults to 1.1 minutes, to allow processing or lock timeout.
        /// </summary>
        public TimeSpan CloseTimeout { get; set; } = TimeSpan.FromMinutes(1.1);

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
        public Action<string> LogAction { private get; set; }

        /// <summary>
        /// Retry policy for client.
        /// </summary>
        public RetryPolicy RetryPolicy { get; set; }

        /// <summary>
        /// (Ignored when using Sessions) Gets or sets the MaxConcurrentCalls that will be passed to the Receiver. Can be null. 
        /// </summary>
        public int? MaxConcurrentCalls { get; set; }

        /// <summary>
        /// Indicates connections are closing
        /// </summary>
        public bool IsClosing { get; set; }


        /// <summary>
        /// Creates a new instance.
        /// </summary>
        /// <param name="context">(Optional) The context that was used to init the Reliable Service that uses this listener.</param>
        /// <param name="serviceBusSendConnectionString">(Optional) A Service Bus connection string that can be used for Sending messages. 
        /// (Returned as Service Endpoint.).
        /// </param>
        /// <param name="serviceBusReceiveConnectionString">(Required) A Service Bus connection string that can be used for Receiving messages. 
        /// </param>
        protected ServiceBusCommunicationListenerBase(ServiceContext context){

            Context = context;
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
        public Task<string> OpenAsync(CancellationToken cancellationToken)
        {
            if (MaxConcurrentCalls.HasValue)
            {
                ProcessingMessage = new SemaphoreSlim(MaxConcurrentCalls.Value, MaxConcurrentCalls.Value);
            }
            else
            {
                ProcessingMessage = new SemaphoreSlim(1, 1);
            }

            return OpenImplAsync(cancellationToken);
        }

        /// <summary>
        /// This method causes the communication listener to close. Close is a terminal state and 
        /// this method allows the communication listener to transition to this state in a
        /// graceful manner.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>
        /// A <see cref="T:System.Threading.Tasks.Task">Task</see> that represents outstanding operation.
        /// </returns>
        public async Task CloseAsync(CancellationToken cancellationToken)
        {
            WriteLog("Service Bus Communication Listener closing");
            IsClosing = true;
            _stopProcessingMessageTokenSource.Cancel();

            //Wait for Message processing to complete..
            await Task.WhenAny(
                // Timeout task.
                Task.Delay(CloseTimeout, cancellationToken),
                // Wait for all processing messages to finish by stealing semaphore entries.
                Task.Run(() =>
                {
                    for (int i = 0; i < (MaxConcurrentCalls ?? 1); i++)
                    {
                        // ReSharper disable once AccessToDisposedClosure
                        // ReSharper disable once EmptyGeneralCatchClause
                        try { ProcessingMessage.Wait(cancellationToken); }
                        catch { }
                    }
                }, cancellationToken));

            ProcessingMessage.Dispose();
            await CloseImplAsync(cancellationToken);
        }


        /// <summary>
        /// This method causes the communication listener to close. Close is a terminal state and
        /// this method causes the transition to close ungracefully. Any outstanding operations
        /// (including close) should be canceled when this method is called.
        /// </summary>
        public virtual void Abort()
        {
            WriteLog("Service Bus Communication Listener aborting");
            Dispose();
        }

        /// <summary>
        /// Starts listening for messages on the configured Service Bus Queue / Subscription
        /// Make sure to call 'base' when overriding.
        /// </summary>
        protected abstract void ListenForMessages();

        /// <summary>
        /// This method causes the communication listener to be opened. Once the Open
        /// completes, the communication listener becomes usable - accepts and sends messages.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>
        /// A <see cref="T:System.Threading.Tasks.Task">Task</see> that represents outstanding operation. The result of the Task is
        /// the endpoint string.
        /// </returns>
        protected virtual Task<string> OpenImplAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult("endpoint://");
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

            ProcessingMessage.Release(MaxConcurrentCalls ?? 1);
            ProcessingMessage.Dispose();
        }

        /// <inheritdoc />
        public abstract Task Complete(Message message);

        /// <inheritdoc />
        public abstract Task Abandon(Message message, IDictionary<string, object> propertiesToModify = null);

        /// <inheritdoc />
        public abstract Task DeadLetter(Message message, IDictionary<string, object> propertiesToModify = null);
    }
}
