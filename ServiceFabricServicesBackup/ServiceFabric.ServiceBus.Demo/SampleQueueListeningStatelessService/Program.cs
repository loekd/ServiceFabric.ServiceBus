using System;
using System.Diagnostics;
using System.Fabric;
using System.Threading;
using Microsoft.ServiceFabric.Services.Runtime;

namespace SampleQueueListeningStatelessService
{
	internal static class Program
	{
		/// <summary>
		/// This is the entry point of the service host process.
		/// </summary>
		private static void Main()
		{
			try
			{
				ServiceRuntime.RegisterServiceAsync("SampleQueueListeningStatelessServiceType", context => new SampleQueueListeningStatelessService(context)).GetAwaiter().GetResult();
				ServiceEventSource.Current.ServiceTypeRegistered(Process.GetCurrentProcess().Id, typeof(SampleQueueListeningStatelessService).Name);
				Thread.Sleep(Timeout.Infinite);  // Prevents this host process from terminating so services keeps running.
			}
			catch (Exception e)
			{
				ServiceEventSource.Current.ServiceHostInitializationFailed(e);
				throw;
			}
		}
	}
}
