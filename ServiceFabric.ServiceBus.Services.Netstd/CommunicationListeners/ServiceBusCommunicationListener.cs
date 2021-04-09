using Microsoft.Azure.ServiceBus;
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
    public abstract class ServiceBusCommunicationListener : ServiceBusCommunicationListenerBase
    {
        /// <summary>
        /// Gets a Service Bus connection string that should have only receive-rights.
        /// </summary>
        protected string ReceiveConnectionString { get; }

        /// <summary>
        /// Gets a Service Bus connection string that should have only send-rights.
        /// </summary>
        protected string SendConnectionString { get; }


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
        ) : base(context)
        {
            if (string.IsNullOrWhiteSpace(serviceBusSendConnectionString))
                serviceBusSendConnectionString = "not:/available";
            if (string.IsNullOrWhiteSpace(serviceBusReceiveConnectionString))
                throw new ArgumentOutOfRangeException(nameof(serviceBusReceiveConnectionString));

            SendConnectionString = serviceBusSendConnectionString;
            ReceiveConnectionString = serviceBusReceiveConnectionString;
        }
    }
}