using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Fluent;

static CosmosClient GetClient() {

    const string connectionString = "AccountEndpoint=https://mslearn-15128244.documents.azure.com:443/;AccountKey=JaLiAGXSehdAWROzDETIp8qWwpwrPdoDDq8n7XhYaJ6pdWXJEbT05rZkYCELYjNYrlOjGB5osuIbACDbq6PlJg==;";

    Console.WriteLine($"[Connection string]:\t{connectionString}");

    CosmosSerializationOptions serializerOptions = new()
    {
        PropertyNamingPolicy = CosmosPropertyNamingPolicy.CamelCase
    };

    CosmosClient output = new CosmosClientBuilder(connectionString)
    .WithSerializerOptions(serializerOptions)
    .Build();

    Console.WriteLine("[Client ready]"); 

    return output;
}

async Task<Container> CreateResourcesAsync() {
    Database database = await client.CreateDatabaseIfNotExistsAsync(
        id: "cosmicworks"
    );
    Console.WriteLine($"[Database created]:\t{database.Id}");

    Container output = await database.CreateContainerIfNotExistsAsync(
        id: "products",
        partitionKeyPath: "/categoryId",
        throughput: 400
    );

    Console.WriteLine($"[Container created]:\t{output.Id}");

    return output;
}

using CosmosClient client = GetClient();
Container container = await CreateResourcesAsync();
