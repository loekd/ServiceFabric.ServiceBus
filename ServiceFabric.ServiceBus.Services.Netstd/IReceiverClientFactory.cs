using Microsoft.Azure.ServiceBus.Core;

namespace ServiceFabric.ServiceBus.Services.Netstd
{
    public interface IReceiverClientFactory
    {
        IReceiverClient Create();
    }
}