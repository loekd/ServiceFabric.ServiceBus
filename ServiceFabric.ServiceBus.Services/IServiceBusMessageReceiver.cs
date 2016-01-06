using Microsoft.ServiceBus.Messaging;

namespace ServiceFabric.ServiceBus.Services
{
	/// <summary>
	/// Marks a class as capable of receiving <see cref="BrokeredMessage"/>s.
	/// </summary>
	public interface IServiceBusMessageReceiver
	{
		/// <summary>
		/// Processes a message.
		/// </summary>
		/// <param name="message"></param>
		void ReceiveMessage(BrokeredMessage message);
	}
}
