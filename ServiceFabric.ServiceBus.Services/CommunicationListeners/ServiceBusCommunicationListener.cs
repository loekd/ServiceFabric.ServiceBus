using System;
using System.Fabric;
using System.Runtime.CompilerServices;
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
        protected ManualResetEvent ProcessingMessage { get; } = new ManualResetEvent(true);

        /// <summary>
        /// Gets the <see cref="ServiceContext"/> that was used to create this instance. Can be null.
        /// </summary>
        protected ServiceContext Context { get; }

        /// <summary>
        /// Gets a Service Bus connection string that should have only receive-rights.
        /// </summary>
        [Obsolete("Replaced by ReceiveConnectionString")]
        protected string ServiceBusReceiveConnectionString => ReceiveConnectionString;

        /// <summary>
        /// Gets a Service Bus connection string that should have only receive-rights.
        /// </summary>
        protected string ReceiveConnectionString { get; }

        /// <summary>
        /// Gets a Service Bus connection string that should have only send-rights.
        /// </summary>
        [Obsolete("Replaced by SendConnectionString")]
        protected string ServiceBusSendConnectionString => SendConnectionString;
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
        /// Indicates whether the Service Bus Queue or Subscription requires sessions.
        /// </summary>
        protected bool RequireSessions { get; private set; }

        /// <summary>
        /// Gets or sets the prefetch size when receiving Service Bus Messages. (Defaults to 0, which indicates no prefetch)
        /// Set to 20 times the total number of messages that a single receiver can process per second.
        /// </summary>
        [Obsolete("Replaced by MessagePrefetchCount")]
        public int ServiceBusMessagePrefetchCount
        {
            get { return MessagePrefetchCount; }
            set { MessagePrefetchCount = value; }
        }

        /// <summary>
        /// Gets or sets the prefetch size when receiving Service Bus Messages. (Defaults to 0, which indicates no prefetch)
        /// Set to 20 times the total number of messages that a single receiver can process per second.
        /// </summary>
        public int MessagePrefetchCount { get; set; }

        /// <summary>
        /// Gets or sets the timeout for receiving a batch of Service Bus Messages. (Defaults to 30s)
        /// </summary>
        [Obsolete("Replaced by ServerTimeout")]
        public TimeSpan ServiceBusServerTimeout
        {
            get { return ServerTimeout; }
            set { ServerTimeout = value; }
        }

        /// <summary>
        /// Gets or sets the timeout for receiving a batch of Service Bus Messages. (Defaults to 30s)
        /// </summary>
        public TimeSpan ServerTimeout { get; set; } = TimeSpan.FromSeconds(30);
        

        /// <summary>
        /// Gets or sets the Service Bus client ReceiveMode. 
        /// </summary>
        [Obsolete("Replaced by ReceiveMode")]
        public ReceiveMode ServiceBusReceiveMode
        {
            get { return ReceiveMode; }
            set { ReceiveMode = value; }
        }

        /// <summary>
        /// Gets or sets the Service Bus client ReceiveMode. 
        /// </summary>
        public ReceiveMode ReceiveMode { get; set; } = ReceiveMode.PeekLock;

        /// <summary>
        /// Gets or sets a callback for writing logs. (Defaults to null)
        /// </summary>
        public Action<string> LogAction { get; set; }

        /// <summary>
        /// Creates a new instance.
        /// </summary>
        /// <param name="context">(Optional) The context that was used to init the Reliable Service that uses this listener.</param>
        /// <param name="serviceBusSendConnectionString">(Optional) A Service Bus connection string that can be used for Sending messages. 
        /// (Returned as Service Endpoint.) When not supplied, an App.config appSettings value with key 'Microsoft.ServiceBus.ConnectionString.Send'
        ///  is used.</param>
        /// <param name="serviceBusReceiveConnectionString">(Optional) A Service Bus connection string that can be used for Receiving messages. 
        ///  When not supplied, an App.config appSettings value with key 'Microsoft.ServiceBus.ConnectionString.Receive'
        ///  is used.</param>
	    /// <param name="requireSessions">Indicates whether the provided Message Queue requires sessions.</param>
        protected ServiceBusCommunicationListener(ServiceContext context
            , string serviceBusSendConnectionString
            , string serviceBusReceiveConnectionString
            , bool requireSessions)
        {
            if (string.IsNullOrWhiteSpace(serviceBusSendConnectionString))
                serviceBusSendConnectionString = CloudConfigurationManager.GetSetting(DefaultSendConnectionStringConfigurationKey);
            if (string.IsNullOrWhiteSpace(serviceBusReceiveConnectionString))
                serviceBusReceiveConnectionString = CloudConfigurationManager.GetSetting(DefaultReceiveConnectionStringConfigurationKey);

            if (string.IsNullOrWhiteSpace(serviceBusSendConnectionString)) serviceBusSendConnectionString = "not:/available";
            if (string.IsNullOrWhiteSpace(serviceBusReceiveConnectionString)) throw new ArgumentOutOfRangeException(nameof(serviceBusReceiveConnectionString));

            RequireSessions = requireSessions;
            Context = context;
            SendConnectionString = serviceBusSendConnectionString;
            ReceiveConnectionString = serviceBusReceiveConnectionString;

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
            WriteLog("Service Bus Communication Listnener closing");
            _stopProcessingMessageTokenSource.Cancel();
            //Wait for Message processing to complete..
            ProcessingMessage.WaitOne();
            ProcessingMessage.Dispose();
            return CloseImplAsync(cancellationToken);
        }

        /// <summary>
        /// This method causes the communication listener to close. Close is a terminal state and
        ///             this method causes the transition to close ungracefully. Any outstanding operations
        ///             (including close) should be canceled when this method is called.
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
            ProcessingMessage.Set();
            ProcessingMessage.Dispose();
            _stopProcessingMessageTokenSource.Cancel();
            _stopProcessingMessageTokenSource.Dispose();
        }
    }
}
