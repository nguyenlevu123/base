package org.example.orderservice.models.services.impl;

import feign.FeignException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import org.example.orderservice.clients.ProductClient;
import org.example.orderservice.exceptions.ProductNotFoundException;
import org.example.orderservice.exceptions.ProductServiceException;
import org.example.orderservice.models.dto.responses.ProductResponse;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ProductGatewayService {

    private final ProductClient productClient;

    @CircuitBreaker(name = "productService", fallbackMethod = "getProductFallback")
    public ProductResponse getProductById(Long productId) {
        try {
            ProductResponse response = productClient.getProductById(productId);
            if (response == null || response.id() == null) {
                throw new ProductNotFoundException(productId);
            }
            return response;
        } catch (FeignException e) {
            if (e.status() == 404) {
                throw new ProductNotFoundException(productId);
            }
            throw new ProductServiceException("Product service error: " + e.getMessage(), e);
        }
    }

    public ProductResponse getProductFallback(Long productId, Throwable throwable) {
        if (throwable instanceof ProductNotFoundException) {
            throw (ProductNotFoundException) throwable;
        }
        if (throwable instanceof FeignException && ((FeignException) throwable).status() == 404) {
            throw new ProductNotFoundException(productId);
        }
        throw new ProductServiceException("Product service is currently unavailable", throwable);
    }
}
