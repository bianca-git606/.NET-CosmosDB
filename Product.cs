public record Product (
    string Id,
    string CategoryId
): Item (
    Id, 
    CategoryId, 
    nameof(Product)
) {
    public string Name { get; init; } = default!;
    public decimal Price { get; init; }
    public bool Archived { get; init; }
    public int Quantity { get; init; }  
}