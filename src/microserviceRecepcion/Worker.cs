/*
 ______         __                                  __          ______  ______     
/\__  _\       /\ \                                /\ \        /\  _  \/\__  _\    
\/_/\ \/    __ \ \ \____   ____   ___    ___    ___\ \ \____   \ \ \L\ \/_/\ \/    
   \ \ \  /'__`\\ \ '__`\ /',__\ /'___\ / __`\ / __`\ \ '__`\   \ \  __ \ \ \ \    
    \ \ \/\ \L\.\\ \ \L\ /\__, `/\ \__//\ \L\ /\ \L\ \ \ \L\ \   \ \ \/\ \ \_\ \__ 
     \ \_\ \__/.\_\ \_,__\/\____\ \____\ \____\ \____/\ \_,__/    \ \_\ \_\/\_____\
      \/_/\/__/\/_/\/___/ \/___/ \/____/\/___/ \/___/  \/___/      \/_/\/_/\/_____/

https://www.tabscoob.ai/ - https://github.com/tabscoob - @devcsar
*/  


using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Options;
using System;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using microserviceRecepcion.Entities;
using Cassandra;
namespace microserviceRecepcion;

public class Worker : BackgroundService
{
    private string topic1 ="alerts";
    private MqttFactory TelemetryFactory = new MqttFactory();
    private readonly ILogger<Worker> _logger;
    public Worker(ILogger<Worker> logger)
    {
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var session =
                Cluster.Builder()
                        .WithCloudSecureConnectionBundle(@"")
                        //or if on linux .WithCloudSecureConnectionBundle(@"/PATH/TO/>>secure-connect.zip")
                        // .AddContactPoints("host")
                        //.WithAuthProvider(new PlainTextAuthProvider("user", password"))
                        .WithCredentials("user", "pass")
                        .Build()
                        .Connect("iot");
                        
         var registro = session.Prepare("insert into paquetes_maskcam (id_dispositivo, hora_recepcion, personas_detectadas, personas_con_cubrebocas, personas_sin_cubrebocas) values (?,?,?,?,?);");
    

        IMqttClient TelemetryClient = TelemetryFactory.CreateMqttClient();
            IMqttClientOptions ConnectionOptions = new MqttClientOptionsBuilder()
            .WithClientId("servicio-recepcion")
            .WithTcpServer("iot.tabscoob.ai")
            // .WithCredentials("user", "password")
            .WithCleanSession()
            .Build();

        TelemetryClient.UseConnectedHandler(async e =>
        {
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine("-- broker: iot.tabscoob.ai --");
                await TelemetryClient.SubscribeAsync(new MqttTopicFilterBuilder().WithTopic(topic1).Build());
                Console.WriteLine($"-- Recibiendo datos de :{topic1}");
        });
        TelemetryClient.UseApplicationMessageReceivedHandler(e =>
        {

                _logger.LogInformation("MESSAGE RECEIVED AT: {time}", DateTimeOffset.Now);
                 Console.ForegroundColor = ConsoleColor.Green;
                 Console.WriteLine($"-- Topic = {e.ApplicationMessage.Topic} --");
                 Console.ForegroundColor = ConsoleColor.DarkYellow;
                 Console.WriteLine($"Paquete = {Encoding.UTF8.GetString(e.ApplicationMessage.Payload)}");
                 string stringPaquete= Encoding.UTF8.GetString(e.ApplicationMessage.Payload);
                 paqueteJetson paquete = JsonSerializer.Deserialize<paqueteJetson>(stringPaquete);
                 TimeSpan hora= TimeSpan.FromSeconds(paquete.timestamp);
                 DateTime horaDeRecepcionDispositivo = new DateTime () + hora;
                 DateTimeOffset horaDeRecepcionServidor = DateTime.Now;
                 DateTimeOffset horaDeRecepcionServidorUTC = DateTime.UtcNow;

                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine($"-- ID del dispistivo = {paquete.device_id} ");
                Console.WriteLine($"-- Personas Detectadas = {paquete.people_total} ");
                Console.WriteLine($"-- Personas con cubrebocas = {paquete.people_with_mask} ");
                Console.WriteLine($"-- Personas sin cubrebocas = {paquete.people_without_mask} ");
                Console.WriteLine($"-- Hora de recepcion(Dispositivo) = {horaDeRecepcionDispositivo} ");
                Console.WriteLine($"-- Hora de recepcion(Servidor) = {horaDeRecepcionServidor} ");

                var statement = registro.Bind(paquete.device_id,horaDeRecepcionServidor,paquete.people_total,paquete.people_with_mask,paquete.people_without_mask);
                var rowSet = session.ExecuteAsync(statement);


                //  var message = new MqttApplicationMessageBuilder()
                //     .WithTopic(topic2)
                //     .WithPayload(Encoding.UTF8.GetString(e.ApplicationMessage.Payload))
                //     .WithAtLeastOnceQoS()
                //     .Build();
                //     Task.Run(() => TelemetryClient.PublishAsync(message));


        });
        TelemetryClient.UseDisconnectedHandler(async e =>
        {
                _logger.LogInformation("-- DISCONNECTED FROM SERVER --");
                await Task.Delay(TimeSpan.FromSeconds(5));

                try
                {
                    await TelemetryClient.ConnectAsync(ConnectionOptions, CancellationToken.None); // Since 3.0.5 with CancellationToken
                }
                catch
                {
                    _logger.LogInformation("-- Fallo la reconeccion --");
                }
        });
        await TelemetryClient.ConnectAsync(ConnectionOptions, CancellationToken.None);
            
    }
    public override Task StopAsync(CancellationToken CancellationToken)
    {
        return base.StopAsync(CancellationToken);
    }
}
