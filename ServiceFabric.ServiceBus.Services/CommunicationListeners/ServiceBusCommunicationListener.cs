using System;
using System.Fabric;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure;
using Microsoft.ServiceBus.Messaging;
using Microsoft.ServiceFabric.Services.Communication.Runtime;

namespace ServiceFabric.ServiceBus.Services.CommunicationListeners
{
    /// <summary>
    /// Abstract base implementation for <see cref="ICommunicationListener"/> connected to ServiceBus
    /// </summary>
    public abstract class ServiceBusCommunicationListener : ICommunicationListener, IDisposable
    {
        private const string DefaultSendConnectionStringConfigurationKey = "Microsoft.ServiceBus.ConnectionString.Send";
        private const string DefaultReceiveConnectionStringConfigurationKey = "Microsoft.ServiceBus.ConnectionString.Receive";
        private readonly CancellationTokenSource _stopProcessingMessageTokenSource;

        //prevents aborts during the processing of a message
        private readonly ManualResetEvent _processingMessage = new ManualResetEvent(true);

        /// <summary>
        /// Gets the processor of incoming messages
        /// </summary>
        protected IServiceBusMessageReceiver Receiver { get; }

        /// <summary>
        /// Gets the <see cref="ServiceContext"/> that was used to create this instance.
        /// </summary>
        protected ServiceContext Context { get; }

        /// <summary>
        /// Gets a Service Bus connection string that should have only receive-rights.
        /// </summary>
        protected string ServiceBusReceiveConnectionString { get; }

        /// <summary>
        /// Gets a Service Bus connection string that should have only send-rights.
        /// </summary>
        protected string ServiceBusSendConnectionString { get; }


        /// <summary>
        /// When <see cref="CancellationToken.IsCancellationRequested"/> is true, this indicates that either <see cref="CloseAsync"/> 
        /// or <see cref="Abort"/> was called.
        /// </summary>
        protected CancellationToken StopProcessingMessageToken { get; }

        /// <summary>
        /// Gets or sets the batch size when receiving Service Bus Messages. (Defaults to 10)
        /// </summary>
        public int ServiceBusMessageBatchSize { get; set; } = 10;

        /// <summary>
        /// Gets or sets the prefetch size when receiving Service Bus Messages. (Defaults to 0, which indicates no prefetch)
        /// Set to 20 times the total number of messages that a single receiver can process per second.
        /// </summary>
        public int ServiceBusMessagePrefetchCount { get; set; } = 0;

        /// <summary>
        /// Gets or sets the timeout for receiving a batch of Service Bus Messages. (Defaults to 30s)
        /// </summary>
        public TimeSpan ServiceBusServerTimeout { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Gets or sets the Service Bus client ReceiveMode. 
        /// </summary>
        public ReceiveMode ServiceBusReceiveMode { get; set; } = ReceiveMode.PeekLock;

        /// <summary>
        /// Creates a new instance.
        /// </summary>
        /// <param name="receiver">(Required) Processes incoming messages.</param>
        /// <param name="context">(Optional) The context that was used to init the Reliable Service that uses this listener.</param>
        /// <param name="serviceBusSendConnectionString">(Optional) A Service Bus connection string that can be used for Sending messages. 
        /// (Returned as Service Endpoint.) When not supplied, an App.config appSettings value with key 'Microsoft.ServiceBus.ConnectionString.Receive'
        ///  is used.</param>
        /// <param name="serviceBusReceiveConnectionString">(Optional) A Service Bus connection string that can be used for Receiving messages. 
        ///  When not supplied, an App.config appSettings value with key 'Microsoft.ServiceBus.ConnectionString.Receive'
        ///  is used.</param>
        protected ServiceBusCommunicationListener(IServiceBusMessageReceiver receiver
            , ServiceContext context
            , string serviceBusSendConnectionString
            , string serviceBusReceiveConnectionString)
        {
            if (context == null) throw new ArgumentNullException(nameof(context));
            if (receiver == null) throw new ArgumentNullException(nameof(receiver));

            if (string.IsNullOrWhiteSpace(serviceBusSendConnectionString))
                serviceBusSendConnectionString = CloudConfigurationManager.GetSetting(DefaultSendConnectionStringConfigurationKey);
            if (string.IsNullOrWhiteSpace(serviceBusReceiveConnectionString))
                serviceBusReceiveConnectionString = CloudConfigurationManager.GetSetting(DefaultReceiveConnectionStringConfigurationKey);

            if (string.IsNullOrWhiteSpace(serviceBusSendConnectionString)) throw new ArgumentOutOfRangeException(nameof(serviceBusSendConnectionString));
            if (string.IsNullOrWhiteSpace(serviceBusReceiveConnectionString)) throw new ArgumentOutOfRangeException(nameof(serviceBusReceiveConnectionString));

            Context = context;
            Receiver = receiver;
            ServiceBusSendConnectionString = serviceBusSendConnectionString;
            ServiceBusReceiveConnectionString = serviceBusReceiveConnectionString;

            _stopProcessingMessageTokenSource = new CancellationTokenSource();
            StopProcessingMessageToken = _stopProcessingMessageTokenSource.Token;
        }

        /// <summary>
        /// This method causes the communication listener to be opened. Once the Open
        ///             completes, the communication listener becomes usable - accepts and sends messages.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>
        /// A <see cref="T:System.Threading.Tasks.Task">Task</see> that represents outstanding operation. The result of the Task is
        ///             the endpoint string.
        /// </returns>
        public abstract Task<string> OpenAsync(CancellationToken cancellationToken);

        /// <summary>
        /// This method causes the communication listener to close. Close is a terminal state and 
        ///             this method allows the communication listener to transition to this state in a
        ///             graceful manner.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>
        /// A <see cref="T:System.Threading.Tasks.Task">Task</see> that represents outstanding operation.
        /// </returns>
        public Task CloseAsync(CancellationToken cancellationToken)
        {
            _stopProcessingMessageTokenSource.Cancel();
            //Wait for Message processing to complete..
            _processingMessage.WaitOne();
            _processingMessage.Dispose();
            return CloseImplAsync(cancellationToken);
        }

        /// <summary>
        /// This method causes the communication listener to close. Close is a terminal state and
        ///             this method causes the transition to close ungracefully. Any outstanding operations
        ///             (including close) should be canceled when this method is called.
        /// </summary>
        public virtual void Abort()
        {
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
        /// Executes the provided <paramref name="action"/> on a background thread.
        /// </summary>
        /// <param name="action"></param>
        protected void StartBackgroundThread(ThreadStart action)
        {
            Thread listener = new Thread(action)
            {
                IsBackground = true
            };
            listener.Start();
        }

        /// <summary>
        /// Will pass an incoming message to the <see cref="Receiver"/> for processing.
        /// </summary>
        /// <param name="message"></param>
        protected async Task ReceiveMessageAsync(BrokeredMessage message)
        {
            try
            {
                _processingMessage.Reset();
                await Receiver.ReceiveMessageAsync(message, StopProcessingMessageToken);
            }
            finally
            {
                message.Dispose();
                _processingMessage.Set();
            }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        protected virtual void Dispose(bool disposing)
        {
            if (!disposing) return;
            _processingMessage.Set();
            _processingMessage.Dispose();
            _stopProcessingMessageTokenSource.Cancel();
            _stopProcessingMessageTokenSource.Dispose();
        }
    }
}
