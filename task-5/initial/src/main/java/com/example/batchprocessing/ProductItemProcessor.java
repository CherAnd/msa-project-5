package com.example.batchprocessing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ProductItemProcessor implements ItemProcessor<Product, Product> {

	private static final Logger log = LoggerFactory.getLogger(ProductItemProcessor.class);

	private final JdbcTemplate jdbcTemplate;

	public ProductItemProcessor(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	@Override
	public Product process(final Product product) {
		String sql = "SELECT loyalityData FROM loyality_data WHERE productSku = ?";
		List<String> result = jdbcTemplate.queryForList(sql, String.class, product.productSku());

		String newProductData = result.isEmpty() ? product.productData() : result.get(0);

		Product transformedProduct = new Product(
				product.productId(),
				product.productSku(),
				product.productName(),
				product.productAmount(),
				newProductData
		);

		log.info("Transforming ({}) into ({})", product, transformedProduct);

		return transformedProduct;
	}
}
