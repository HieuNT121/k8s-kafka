using Microsoft.AspNetCore.Mvc;
using OrderApi.Models;
using OrderApi.Services;

namespace OrderApi.Controllers;

[ApiController]
[Route("orders")]
public class OrdersController : ControllerBase
{
    private readonly KafkaProducerService _producer;

    public OrdersController(
        KafkaProducerService producer)
    {
        _producer = producer;
    }

    [HttpPost]
    public async Task<IActionResult> Create(
        CreateOrderRequest request)
    {
        var orderId =
            $"ORD-{Guid.NewGuid():N}";

        var orderEvent =
            new OrderCreatedEvent
            {
                EventId = Guid.NewGuid().ToString(),
                EventType = "OrderCreated",
                OccurredAt = DateTime.UtcNow,
                OrderId = orderId,
                CustomerId = request.CustomerId,
                ProductId = request.ProductId,
                Quantity = request.Quantity
            };

        await _producer.ProduceAsync(
            orderEvent);

        return Ok(new
        {
            orderId,
            status = "created"
        });
    }
}