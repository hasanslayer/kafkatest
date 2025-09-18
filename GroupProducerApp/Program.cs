using Confluent.Kafka;

namespace GroupProducerApp
{
    internal class Program
    {
        static async Task Main()
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

            using var producer = new ProducerBuilder<string, string>(config).Build();

            Console.WriteLine("Type messages to send to Kafka (type 'exit' to quit):");

            try
            {
                for (int i = 0; i < 100; i++)
                {
                    var msg = new Message<string, string> { Value = $"abcdefghijklmopqrstuvwxyz" };
                    var result = await producer.ProduceAsync("my-big-topic", msg);
                    Console.WriteLine(
                        $"Delivered '{result.Value}' to '{result.TopicPartitionOffset}'"
                    );
                }
            }
            catch (ProduceException<Null, string> e)
            {
                Console.WriteLine($"Delivery failed: {e.Error.Reason}");
            }
        }
    }
}
