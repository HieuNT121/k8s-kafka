namespace OrderApi.Models;

public class OrderCreatedEvent
{
    public string EventId { get; set; } = string.Empty;

    public string EventType { get; set; } = "OrderCreated";

    public DateTime OccurredAt { get; set; }

    public string OrderId { get; set; } = string.Empty;

    public string CustomerId { get; set; } = string.Empty;

    public string ProductId { get; set; } = string.Empty;

    public int Quantity { get; set; }
}