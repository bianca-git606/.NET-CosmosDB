using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Fluent;
using Microsoft.Azure.Cosmos.Linq;

static CosmosClient GetClient() {

    const string connectionString = "<Account Endpoint> <AccountKey>";

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


using CosmosClient client = GetClient();

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

Container container = await CreateResourcesAsync();

async Task CreateItemsAsync() {

    Category goggles = new(
        Id: "ef7fa0f1-0e9d-4435-aaaf-a778179a94ad",
        CategoryId: "gear-snow-goggles"
    );

    PartitionKey gogglesKey = new("gear-snow-goggles");
    Category result = await container.UpsertItemAsync(goggles, gogglesKey);

    Console.WriteLine($"[New item created]:\t{result.Id}\t(Type: {result.Type})");

    Category helmets = new(
        Id: "91f79374-8611-4505-9c28-3bbbf1aa7df7",
        CategoryId: "gear-climb-helmets"
    );

    PartitionKey helmetsKey = new("gear-climb-helmets");
    ItemResponse<Category> response = await container.UpsertItemAsync(helmets, helmetsKey);
    Console.WriteLine($"[New item created]:\t{response.Resource.Id}\t(Type: {response.Resource.Type})\t(RUs: {response.RequestCharge})");
}

await CreateItemsAsync();

async Task CreateItemBatchAsync() {

    Category tents = new (
        Id: "5df21ec5-813c-423e-9ee9-1a2aaead0be4",
        CategoryId: "gear-camp-tents"
    );

    Product cirroa = new Product(
        Id: "e8dddee4-9f43-4d15-9b08-0d7f36adcac8",
        CategoryId: "gear-camp-tents"
    ) {
        Name = "Cirroa Tent",
        Price = 490.00m,
        Archived = false,
        Quantity = 15
    };

    Product kuloar = new(
        Id: "e6f87b8d-8cd7-4ade-a005-14d3e2fbd1aa",
        CategoryId: "gear-camp-tents"
    ){
        Name = "Kuloar Tent",
        Price = 530.00m,
        Archived = false,
        Quantity = 8
    };

    Product mammatin = new(
        Id: "f7653468-c4b8-47c9-97ff-451ee55f4fd5",
        CategoryId: "gear-camp-tents"
    ){
        Name = "Mammatin Tent",
        Price = 0.00m,
        Archived = true,
        Quantity = 0
    };

    Product nimbolo = new(
        Id: "6e3b7275-57d4-4418-914d-14d1baca0979",
        CategoryId: "gear-camp-tents"
    ){
        Name = "Nimbolo Tent",
        Price = 330.00m,
        Archived = false,
        Quantity = 35
    };

    PartitionKey tentsKey = new("gear-camp-tents");

    TransactionalBatch batch = container.CreateTransactionalBatch(tentsKey)
    .UpsertItem<Category>(tents)
    .UpsertItem<Product>(cirroa)
    .UpsertItem<Product>(kuloar)
    .UpsertItem<Product>(mammatin)
    .UpsertItem<Product>(nimbolo);
    Console.WriteLine("[Batch started]");

    using TransactionalBatchResponse batchResponse = await batch.ExecuteAsync();
    for (int i = 0; i < batchResponse.Count; i++) {

        TransactionalBatchOperationResult<Item> result = batchResponse.GetOperationResultAtIndex<Item>(i);
        Console.WriteLine($"[New item created]:\t{result.Resource.Id}\t(Type: {result.Resource.Type})");

    }
    Console.WriteLine($"[Batch completed]:\t(RUs: {batchResponse.RequestCharge})");
}

await CreateItemBatchAsync();

async Task PointReadAsync() {

    PartitionKey readKey = new("gear-climb-helmets");

    ItemResponse<Category> readResponse = await container.ReadItemAsync<Category>(
        id: "91f79374-8611-4505-9c28-3bbbf1aa7df7",
        partitionKey: readKey
    );

    Category readItem = readResponse.Resource;

    Console.WriteLine($"[Point read item]:\t{readItem.Id}\t(RUs: {readResponse.RequestCharge})");  
}

await PointReadAsync();

async Task QueryAsync() {

    // Execute a query
    string statement = "SELECT * FROM products p WHERE p.categoryId = @partitionKey";

    var query = new QueryDefinition(
        query: statement
    );

    var parameterizedQuery = query.WithParameter("@partitionKey", "gear-camp-tents");

    using FeedIterator<Product> feed = container.GetItemQueryIterator<Product>(
        queryDefinition: parameterizedQuery
    );

    Console.WriteLine($"[Start query]:\t{statement}");

    double totalRequestCharge = 0d;

    while (feed.HasMoreResults)
    {
        FeedResponse<Product> page = await feed.ReadNextAsync();

        totalRequestCharge += page.RequestCharge;

        foreach (Product item in page)
        {
            Console.WriteLine($"[Returned item]:\t{item.Id}\t(Name: {item.Name ?? "N/A"})");
        }
    }

    Console.WriteLine($"[Query metrics]:\t(RUs: {totalRequestCharge})");
}

await QueryAsync();

async Task QueryLINQAsync() {

    // Query using LINQ expressions
    IOrderedQueryable<Product> queryable = container.GetItemLinqQueryable<Product>();
    var matches = queryable
    .Where(p => p.Type == nameof(Product))
    .Where(p => !p.Archived)
    .OrderBy(p => p.Price);

    using FeedIterator<Product> linqFeed = matches.ToFeedIterator();
    Console.WriteLine($"[Start LINQ query]");

    // Paginate LINQ query results
    while (linqFeed.HasMoreResults){
        FeedResponse<Product> page = await linqFeed.ReadNextAsync();
        Console.WriteLine($"[Page RU charge]:\t{page.RequestCharge}");
        foreach(Product item in page) {
            Console.WriteLine($"[Returned item]:\t{item}");
        }
    }
}

await QueryLINQAsync();