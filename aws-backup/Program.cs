// See https://aka.ms/new-console-template for more information

using Microsoft.Extensions.Hosting;

var host = Host.CreateDefaultBuilder(args)
    .UseWindowsService() // ← this enables service integration
    .UseSystemd() // ← this enables systemd integration
    .ConfigureServices(services =>
    {
        // ... your AddHostedService<...>() etc.
    })
    .Build();

await host.RunAsync();

