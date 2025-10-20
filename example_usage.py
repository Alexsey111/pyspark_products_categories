#!/usr/bin/env python3
"""
Пример использования PySpark приложения для работы с продуктами и категориями.

Демонстрирует основные возможности приложения.
"""

from pyspark.sql import SparkSession
from pyspark_app.products_categories import ProductsCategoriesProcessor, create_sample_data


def main():
    """Основная функция с примерами использования."""
    print("=== PySpark Products and Categories Demo ===\n")
    
    # Создаем Spark сессию
    spark = SparkSession.builder \
        .appName("ProductsCategoriesDemo") \
        .master("local[2]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    
    # Отключаем логи для чистоты вывода
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        # Создаем процессор
        processor = ProductsCategoriesProcessor(spark)
        
        # Создаем примеры данных
        print("1. Создание тестовых данных...")
        products_df, categories_df, product_categories_df = create_sample_data(spark)
        
        print(f"   Продуктов: {products_df.count()}")
        print(f"   Категорий: {categories_df.count()}")
        print(f"   Связей: {product_categories_df.count()}")
        print()
        
        # Показываем исходные данные
        print("2. Исходные данные:")
        print("   Продукты:")
        products_df.show()
        
        print("   Категории:")
        categories_df.show()
        
        print("   Связи продукт-категория:")
        product_categories_df.show()
        print()
        
        # Получаем все пары продукт-категория
        print("3. Все пары 'Продукт - Категория':")
        all_pairs_df = processor.get_products_with_categories(
            products_df, categories_df, product_categories_df
        )
        all_pairs_df.show(truncate=False)
        print()
        
        # Получаем продукты без категорий
        print("4. Продукты без категорий:")
        products_without_categories_df = processor.get_products_without_categories(
            products_df, product_categories_df
        )
        products_without_categories_df.show(truncate=False)
        print()
        
        # Демонстрируем работу с продуктами без категорий
        print("5. Создание данных с продуктами без категорий...")
        
        # Создаем дополнительные данные с продуктами без категорий
        additional_products_data = [
            (9, "Uncategorized Item 1"),
            (10, "Uncategorized Item 2")
        ]
        additional_products_df = spark.createDataFrame(
            additional_products_data, ["product_id", "product_name"]
        )
        
        # Объединяем с существующими продуктами
        all_products_df = products_df.union(additional_products_df)
        
        print("   Обновленный список продуктов:")
        all_products_df.show()
        print()
        
        # Получаем обновленные результаты
        print("6. Обновленные пары 'Продукт - Категория' (включая продукты без категорий):")
        updated_pairs_df = processor.get_products_with_categories(
            all_products_df, categories_df, product_categories_df
        )
        updated_pairs_df.show(truncate=False)
        print()
        
        print("7. Продукты без категорий (обновленный список):")
        updated_products_without_categories_df = processor.get_products_without_categories(
            all_products_df, product_categories_df
        )
        updated_products_without_categories_df.show(truncate=False)
        print()
        
        # Демонстрируем SQL запросы
        print("8. Демонстрация SQL запросов:")
        
        # Создаем временные представления
        all_products_df.createOrReplaceTempView("products")
        categories_df.createOrReplaceTempView("categories")
        product_categories_df.createOrReplaceTempView("product_categories")
        
        # Пример сложного запроса: продукты с количеством категорий
        complex_query = """
        SELECT 
            p.product_name,
            COUNT(c.category_name) as category_count,
            COLLECT_LIST(c.category_name) as categories
        FROM products p
        LEFT JOIN product_categories pc ON p.product_id = pc.product_id
        LEFT JOIN categories c ON pc.category_id = c.category_id
        GROUP BY p.product_id, p.product_name
        ORDER BY category_count DESC, p.product_name
        """
        
        complex_result_df = spark.sql(complex_query)
        print("   Продукты с количеством категорий:")
        complex_result_df.show(truncate=False)
        print()
        
        # Статистика
        print("9. Статистика:")
        total_products = all_products_df.count()
        products_with_categories = updated_pairs_df.filter(
            updated_pairs_df.category_name.isNotNull()
        ).select("product_name").distinct().count()
        products_without_categories = updated_products_without_categories_df.count()
        
        print(f"   Всего продуктов: {total_products}")
        print(f"   Продуктов с категориями: {products_with_categories}")
        print(f"   Продуктов без категорий: {products_without_categories}")
        print(f"   Процент покрытия категориями: {(products_with_categories/total_products)*100:.1f}%")
        
    finally:
        # Закрываем Spark сессию
        spark.stop()
        print("\n=== Demo завершена ===")


if __name__ == "__main__":
    main()
