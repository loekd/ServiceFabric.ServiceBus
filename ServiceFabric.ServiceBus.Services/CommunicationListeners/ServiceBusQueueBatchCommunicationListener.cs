using System;
using System.Collections.Generic;
using System.Fabric;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using System.Linq;

namespace ServiceFabric.ServiceBus.Services.CommunicationListeners
{
    /// <summary>
    /// Implementation of <see cref="ICommunicationListener"/> that listens to a Service Bus Queue.
    /// </summary>
    public class ServiceBusQueueBatchCommunicationListener : ServiceBusQueueCommunicationListenerBase
    {
        /// <summary>
        /// (Obsolete) Gets or sets the batch size when receiving Service Bus Messages. (Defaults to 10)
        /// </summary>
        [Obsolete("Replaced by MessageBatchSize")]
        public int ServiceBusMessageBatchSize
        {
            get { return MessageBatchSize; }
            set { MessageBatchSize = value; }
        }

        /// <summary>
        /// Gets or sets the batch size when receiving Service Bus Messages. (Defaults to 10)
        /// </summary>
        public int MessageBatchSize { get; set; } = 10;

        /// <summary>
        /// Processor for messages.
        /// </summary>
        protected IServiceBusMessageBatchReceiver BatchReceiver { get; }

        /// <summary>
        /// Gets or set the interval at which message locks are renewed, while a batch is being processed. 
        /// Set to the configured message lock duration (e.g. QueueDescription.LockDuration), minus a clock skew. (50 seconds works well when lockduration is 60s.) 
        /// Set to null if no locks need to be renewed. (Defaults to null.)
        /// </summary>
        public TimeSpan? MessageLockRenewTimeSpan { get; set; }

        /// <summary>
	    /// Creates a new instance, using the init parameters of a <see cref="StatefulService"/>
	    /// </summary>
	    /// <param name="receiver">(Required) Processes incoming message batches.</param>
	    /// <param name="context">(Optional) The context that was used to init the Reliable Service that uses this listener.</param>
	    /// <param name="serviceBusQueueName">The name of the monitored Service Bus Queue</param>
	    /// <param name="serviceBusSendConnectionString">(Optional) A Service Bus connection string that can be used for Sending messages. 
	    /// (Returned as Service Endpoint.) When not supplied, an App.config appSettings value with key 'Microsoft.ServiceBus.ConnectionString.Receive'
	    ///  is used.</param>
	    /// <param name="serviceBusReceiveConnectionString">(Optional) A Service Bus connection string that can be used for Receiving messages. 
	    ///  When not supplied, an App.config appSettings value with key 'Microsoft.ServiceBus.ConnectionString.Receive'
	    ///  is used.</param>
	    /// <param name="requireSessions">Indicates whether the provided Message Queue requires sessions.</param>
        public ServiceBusQueueBatchCommunicationListener(IServiceBusMessageBatchReceiver receiver, ServiceContext context, string serviceBusQueueName, string serviceBusSendConnectionString = null, string serviceBusReceiveConnectionString = null, bool requireSessions = false)
            : base(context, serviceBusQueueName, serviceBusSendConnectionString, serviceBusReceiveConnectionString, requireSessions)
        {
            BatchReceiver = receiver;
        }

        /// <summary>
        /// Starts listening for messages on the configured Service Bus Queue.
        /// </summary>
        protected override void ListenForMessages()
        {
            ThreadStart ts = async () =>
            {
                WriteLog($"Service Bus Communication Listnener now listening for messages.");

                while (!StopProcessingMessageToken.IsCancellationRequested)
                {
                    var messages = ServiceBusClient.ReceiveBatch(MessageBatchSize, ServerTimeout).ToArray();
                    if (messages.Length == 0) continue;

                    var tokens = BatchReceiver.AutoComplete ? messages.Select(m => m.LockToken).ToArray() : null;

                    await ProcessMessagesAsync(messages, null);

                    if (BatchReceiver.AutoComplete)
                    {
                        await ServiceBusClient.CompleteBatchAsync(tokens);
                    }
                }
            };
            StartBackgroundThread(ts);
        }

        /// <summary>
        /// Starts listening for session messages on the configured Service Bus Queue.
        /// </summary>
        protected override void ListenForSessionMessages()
        {
            ThreadStart ts = async () =>
            {
                WriteLog($"Service Bus Communication Listnener now listening for session messages.");

                while (!StopProcessingMessageToken.IsCancellationRequested)
                {
                    MessageSession session = null;
                    try
                    {
                        session = ServiceBusClient.AcceptMessageSession(ServerTimeout);
                        
                        do
                        {
                            var messages = session.ReceiveBatch(MessageBatchSize, ServerTimeout).ToArray();
                            if (messages.Length == 0) break;

                            var tokens = BatchReceiver.AutoComplete ? messages.Select(m => m.LockToken).ToArray() : null;

                            await ProcessMessagesAsync(messages, session);

                            if (BatchReceiver.AutoComplete)
                            {
                                await session.CompleteBatchAsync(tokens);
                            }
                        }
                        while (!StopProcessingMessageToken.IsCancellationRequested);
                    }
                    catch (TimeoutException)
                    { }
                    finally
                    {
                        session?.Close();
                    }
                }
            };
            StartBackgroundThread(ts);
        }

        /// <summary>
        /// Processes the provided set of <see cref="BrokeredMessage"/>s, with optional automatic lock renewal.
        /// </summary>
	    /// <param name="messageSession">Contains the MessageSession when sessions are enabled.</param>
        /// <param name="messages"></param>
        /// <returns></returns>
        private async Task ProcessMessagesAsync(ICollection<BrokeredMessage> messages, MessageSession messageSession)
        {
            WriteLog($"Service Bus Communication Listnener processing {messages.Count} messages.");

            using (CreateRenewTimer(messages))
            {
                try
                {
                    ProcessingMessage.Reset();
                    await BatchReceiver.ReceiveMessagesAsync(messages, messageSession, StopProcessingMessageToken);
                }
                finally
                {
                    foreach (var message in messages)
                    {
                        message.Dispose();
                    }
                    ProcessingMessage.Set();
                }
            }
        }

        /// <summary>
        /// Executes the provided <paramref name="action"/> on a background thread.
        /// </summary>
        /// <param name="action"></param>
        protected void StartBackgroundThread(ThreadStart action)
        {
            var listener = new Thread(action)
            {
                IsBackground = true
            };
            listener.Start();
        }

        /// <summary>
        /// Returns a set of timers that will be used to renew message locks, if <see cref="MessageLockRenewTimeSpan"/> has a value.
        /// </summary>
        /// <param name="messages"></param>
        /// <returns></returns>
        private MessageLockRenewTimerSet CreateRenewTimer(ICollection<BrokeredMessage> messages)
        {
            var timers = MessageLockRenewTimeSpan.HasValue ? new MessageLockRenewTimerSet(messages, MessageLockRenewTimeSpan.Value, LogAction) : new MessageLockRenewTimerSet();
            return timers;
        }
    }
}