using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Services.Client;
using Microsoft.ServiceFabric.Services.Communication.Client;

namespace ServiceFabric.ServiceBus.Clients
{
	/// <summary>
	/// Factory that creates instances of ServiceBusQueueCommunicationClient to communicate to ServiceFabric Services 
	/// using a Service Bus Topic.
	/// </summary>
	public class ServiceBusTopicCommunicationClientFactory : CommunicationClientFactoryBase<ServiceBusTopicCommunicationClient>
	{
		private readonly string _topicName;

		/// <summary>
		/// Creates a new instance, using the provided <see cref="ServicePartitionResolver"/> and Service Bus Topic name.
		/// </summary>
		/// <param name="resolver"></param>
		/// <param name="topicName"></param>
		public ServiceBusTopicCommunicationClientFactory(ServicePartitionResolver resolver, string topicName)
			: base(resolver)
		{
			_topicName = topicName;
		}

		/// <summary>
		/// Returns true if the client is still valid. Connection oriented transports can use this method to indicate that the client is no longer
		///             connected to the service.
		/// </summary>
		/// <param name="client">the communication client</param>
		/// <returns>
		/// true if the client is valid, false otherwise
		/// </returns>
		protected override bool ValidateClient(ServiceBusTopicCommunicationClient client)
		{
			return client != null;
		}

		/// <summary>
		/// Returns true if the client is still valid and connected to the endpoint specified in the parameter.
		/// </summary>
		/// <param name="endpoint">the endpoint to which the </param><param name="client">the communication client</param>
		/// <returns>
		/// true if the client is valid, false otherwise
		/// </returns>
		protected override bool ValidateClient(string endpoint, ServiceBusTopicCommunicationClient client)
		{
			return client != null && !string.IsNullOrWhiteSpace(endpoint);
		}

		/// <summary>
		/// Creates a communication client for the given endpoint address.
		/// </summary>
		/// <param name="endpoint">Endpoint address where the service is listening</param><param name="cancellationToken">Cancellation token</param>
		/// <returns>
		/// The communication client that was created
		/// </returns>
		protected override Task<ServiceBusTopicCommunicationClient> CreateClientAsync(string endpoint, CancellationToken cancellationToken)
		{
			var result = Task.FromResult(new ServiceBusTopicCommunicationClient(endpoint, _topicName));
			return result;
		}

		/// <summary>
		/// Aborts the given client
		/// </summary>
		/// <param name="client">Communication client</param>
		protected override void AbortClient(ServiceBusTopicCommunicationClient client)
		{
			client.AbortClient();
		}
	}
}