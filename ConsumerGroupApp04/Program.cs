using Confluent.Kafka;

namespace ConsumerGroupApp04
{
    internal class Program
    {
        static void Main(string[] args)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "test-group",
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };

            using var consumer = new ConsumerBuilder<Null, string>(config).Build();

            List<string> topics = new List<string>();
            topics.Add("my-big-topic");

            consumer.Subscribe(topics);

            Console.WriteLine("Waiting for messages...");

            while (true)
            {
                var cr = consumer.Consume(10);
                //Console.WriteLine(
                //    $"Consumed message '{cr?.Value}' from '{cr?.TopicPartitionOffset}' in a topic '{cr?.Topic}' "
                //);
                if (cr is not null)
                {
                    Console.WriteLine(
                        $"topic: '{cr?.Topic}' partition: '{cr?.Partition}' value: '{cr?.Message.Value.ToUpper()}'"
                    );
                }
            }
        }
    }
}
