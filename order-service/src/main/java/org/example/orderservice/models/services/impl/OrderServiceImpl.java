package org.example.orderservice.models.services.impl;

import lombok.RequiredArgsConstructor;
import org.example.orderservice.models.constants.OrderStatus;
import org.example.orderservice.models.dto.requests.CreateOrderDetailRequest;
import org.example.orderservice.models.dto.requests.CreateOrderRequest;
import org.example.orderservice.models.dto.responses.OrderDetailResponse;
import org.example.orderservice.models.dto.responses.OrderResponse;
import org.example.orderservice.models.dto.responses.ProductResponse;
import org.example.orderservice.models.entities.Order;
import org.example.orderservice.models.entities.OrderDetail;
import org.example.orderservice.models.repositories.OrderDetailRepository;
import org.example.orderservice.models.repositories.OrderRepository;
import org.example.orderservice.models.services.OrderService;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class OrderServiceImpl implements OrderService {

    private final OrderRepository orderRepository;
    private final OrderDetailRepository orderDetailRepository;
    private final ProductGatewayService productGatewayService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Override
    @Transactional
    public OrderResponse createOrder(CreateOrderRequest request) {
        double total = 0.0;
        List<ProductResponse> productResponses = new ArrayList<>();

        for (CreateOrderDetailRequest item : request.items()) {
            ProductResponse product = productGatewayService.getProductById(item.productId());
            productResponses.add(product);
            total += product.price() * item.quantity();
        }

        Order order = Order.builder()
                .customerName(request.customerName())
                .total(total)
                .status(OrderStatus.PENDING)
                .build();
        Order savedOrder = orderRepository.save(order);

        List<OrderDetailResponse> detailResponses = new ArrayList<>();
        for (int i = 0; i < request.items().size(); i++) {
            CreateOrderDetailRequest item = request.items().get(i);
            ProductResponse product = productResponses.get(i);

            OrderDetail orderDetail = OrderDetail.builder()
                    .order(savedOrder)
                    .productId(item.productId())
                    .quantity(item.quantity())
                    .unitPrice(product.price())
                    .build();
            OrderDetail savedDetail = orderDetailRepository.save(orderDetail);

            detailResponses.add(new OrderDetailResponse(
                    savedDetail.getId(),
                    product.id(),
                    product.name(),
                    item.quantity(),
                    product.price(),
                    product.price() * item.quantity()
            ));
        }

        kafkaTemplate.send("order-created", request.customerEmail());

        return new OrderResponse(
                savedOrder.getId(),
                savedOrder.getCustomerName(),
                savedOrder.getTotal(),
                savedOrder.getStatus(),
                detailResponses
        );
    }
}
