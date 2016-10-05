using System.Fabric;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using Microsoft.ServiceFabric.Services.Communication.Client;

namespace ServiceFabric.ServiceBus.Clients
{
    /// <summary>
    /// An implementation of <see cref="ICommunicationClient"/> that can communicate to ServiceFabric Services through a Service Bus Queue.
    /// </summary>
    public class ServiceBusQueueCommunicationClient : ICommunicationClient
    {
        private readonly string _serviceUri;
        private readonly string _queueName;
        private QueueClient _sendClient;

        /// <summary>
        /// Gets or Sets the Resolved service partition which was used when this client was created.
        /// </summary>
        /// <value>
        /// <see cref="T:System.Fabric.ResolvedServicePartition"/> object
        /// </value>
        public ResolvedServicePartition ResolvedServicePartition { get; set; }

        /// <summary>
        /// Gets or Sets the name of the listener in the replica or instance to which the client is
        /// connected to.
        /// </summary>
        public string ListenerName { get; set; }

        /// <summary>
        /// Gets or Sets the service endpoint to which the client is connected to.
        /// </summary>
        /// <value>
        /// <see cref="T:System.Fabric.ResolvedServiceEndpoint"/>
        /// </value>
        public ResolvedServiceEndpoint Endpoint { get; set; }

        /// <summary>
        /// Creates a new instance using the provided ServiceFabric Service Uri and Service Bus Queue name.
        /// </summary>
        /// <param name="serviceUri"></param>
        /// <param name="queueName"></param>
        public ServiceBusQueueCommunicationClient(string serviceUri, string queueName)
        {
            _serviceUri = serviceUri;
            _queueName = queueName;
        }

        /// <summary>
        /// Sends a message to the ServiceFabric Service.
        /// </summary>
        /// <returns></returns>
        public Task SendMessageAsync(BrokeredMessage message)
        {
            CreateClient();
            return _sendClient.SendAsync(message);
        }
        

        /// <summary>
        /// Sends a message to the ServiceFabric Service.
        /// </summary>
        /// <returns></returns>
        public void SendMessage(BrokeredMessage message)
        {
            CreateClient();
            _sendClient.Send(message);
        }

        /// <summary>
        /// Closes the connection to the ServiceFabric Service
        /// </summary>
        public void AbortClient()
        {
            _sendClient?.Abort();
        }

        private void CreateClient()
        {
            if (_sendClient != null)
                return;

            if (string.IsNullOrWhiteSpace(_queueName))
            {
                _sendClient = QueueClient.CreateFromConnectionString(_serviceUri);
            }
            else
            {
                _sendClient = QueueClient.CreateFromConnectionString(_serviceUri, _queueName);
            }
        }
    }
}
