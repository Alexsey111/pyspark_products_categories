"""
Модуль для работы с продуктами и категориями в PySpark.

Содержит методы для получения связей между продуктами и категориями.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, isnan, isnull, lit
from typing import List, Tuple


class ProductsCategoriesProcessor:
    """Класс для обработки данных о продуктах и категориях."""
    
    def __init__(self, spark: SparkSession):
        """
        Инициализация процессора.
        
        Args:
            spark: Сессия Spark
        """
        self.spark = spark
    
    def get_products_with_categories(
        self, 
        products_df: DataFrame, 
        categories_df: DataFrame, 
        product_categories_df: DataFrame
    ) -> DataFrame:
        """
        Получает все пары "Имя продукта – Имя категории" и продукты без категорий.
        
        Args:
            products_df: DataFrame с продуктами (должен содержать колонки 'product_id', 'product_name')
            categories_df: DataFrame с категориями (должен содержать колонки 'category_id', 'category_name')
            product_categories_df: DataFrame со связями (должен содержать колонки 'product_id', 'category_id')
        
        Returns:
            DataFrame с колонками 'product_name' и 'category_name'
            Для продуктов без категорий category_name будет NULL
        """
        # Проверяем наличие необходимых колонок
        self._validate_dataframes(products_df, categories_df, product_categories_df)
        
        # Создаем временные представления для удобства работы
        products_df.createOrReplaceTempView("products")
        categories_df.createOrReplaceTempView("categories")
        product_categories_df.createOrReplaceTempView("product_categories")
        
        # SQL запрос для получения всех пар продукт-категория и продуктов без категорий
        query = """
        SELECT 
            p.product_name,
            c.category_name
        FROM products p
        LEFT JOIN product_categories pc ON p.product_id = pc.product_id
        LEFT JOIN categories c ON pc.category_id = c.category_id
        ORDER BY p.product_name, c.category_name
        """
        
        result_df = self.spark.sql(query)
        
        return result_df
    
    def get_products_without_categories(
        self, 
        products_df: DataFrame, 
        product_categories_df: DataFrame
    ) -> DataFrame:
        """
        Получает список продуктов, у которых нет категорий.
        
        Args:
            products_df: DataFrame с продуктами
            product_categories_df: DataFrame со связями продукт-категория
        
        Returns:
            DataFrame с колонкой 'product_name' продуктов без категорий
        """
        # Проверяем наличие необходимых колонок
        self._validate_products_and_links(products_df, product_categories_df)
        
        # Создаем временные представления
        products_df.createOrReplaceTempView("products")
        product_categories_df.createOrReplaceTempView("product_categories")
        
        # SQL запрос для получения продуктов без категорий
        query = """
        SELECT DISTINCT p.product_name
        FROM products p
        LEFT JOIN product_categories pc ON p.product_id = pc.product_id
        WHERE pc.product_id IS NULL
        ORDER BY p.product_name
        """
        
        result_df = self.spark.sql(query)
        
        return result_df
    
    def get_all_products_and_categories(
        self, 
        products_df: DataFrame, 
        categories_df: DataFrame, 
        product_categories_df: DataFrame
    ) -> DataFrame:
        """
        Получает полный список всех продуктов с их категориями (включая продукты без категорий).
        
        Args:
            products_df: DataFrame с продуктами
            categories_df: DataFrame с категориями
            product_categories_df: DataFrame со связями продукт-категория
        
        Returns:
            DataFrame с колонками 'product_name' и 'category_name'
        """
        return self.get_products_with_categories(products_df, categories_df, product_categories_df)
    
    def _validate_dataframes(
        self, 
        products_df: DataFrame, 
        categories_df: DataFrame, 
        product_categories_df: DataFrame
    ) -> None:
        """
        Проверяет наличие необходимых колонок в DataFrame'ах.
        
        Args:
            products_df: DataFrame с продуктами
            categories_df: DataFrame с категориями
            product_categories_df: DataFrame со связями
        
        Raises:
            ValueError: Если отсутствуют необходимые колонки
        """
        # Проверяем колонки в products_df
        required_product_cols = ['product_id', 'product_name']
        missing_product_cols = [col for col in required_product_cols if col not in products_df.columns]
        if missing_product_cols:
            raise ValueError(f"Отсутствуют колонки в products_df: {missing_product_cols}")
        
        # Проверяем колонки в categories_df
        required_category_cols = ['category_id', 'category_name']
        missing_category_cols = [col for col in required_category_cols if col not in categories_df.columns]
        if missing_category_cols:
            raise ValueError(f"Отсутствуют колонки в categories_df: {missing_category_cols}")
        
        # Проверяем колонки в product_categories_df
        required_link_cols = ['product_id', 'category_id']
        missing_link_cols = [col for col in required_link_cols if col not in product_categories_df.columns]
        if missing_link_cols:
            raise ValueError(f"Отсутствуют колонки в product_categories_df: {missing_link_cols}")
    
    def _validate_products_and_links(
        self, 
        products_df: DataFrame, 
        product_categories_df: DataFrame
    ) -> None:
        """
        Проверяет наличие необходимых колонок в DataFrame'ах продуктов и связей.
        
        Args:
            products_df: DataFrame с продуктами
            product_categories_df: DataFrame со связями
        
        Raises:
            ValueError: Если отсутствуют необходимые колонки
        """
        # Проверяем колонки в products_df
        required_product_cols = ['product_id', 'product_name']
        missing_product_cols = [col for col in required_product_cols if col not in products_df.columns]
        if missing_product_cols:
            raise ValueError(f"Отсутствуют колонки в products_df: {missing_product_cols}")
        
        # Проверяем колонки в product_categories_df
        required_link_cols = ['product_id', 'category_id']
        missing_link_cols = [col for col in required_link_cols if col not in product_categories_df.columns]
        if missing_link_cols:
            raise ValueError(f"Отсутствуют колонки в product_categories_df: {missing_link_cols}")


def create_sample_data(spark: SparkSession) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Создает примеры данных для тестирования.
    
    Args:
        spark: Сессия Spark
    
    Returns:
        Кортеж из трех DataFrame: (products_df, categories_df, product_categories_df)
    """
    # Создаем данные о продуктах
    products_data = [
        (1, "Laptop"),
        (2, "Mouse"),
        (3, "Keyboard"),
        (4, "Monitor"),
        (5, "Headphones"),
        (6, "Tablet"),
        (7, "Phone"),
        (8, "Camera")
    ]
    products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])
    
    # Создаем данные о категориях
    categories_data = [
        (1, "Electronics"),
        (2, "Computer Accessories"),
        (3, "Audio"),
        (4, "Mobile Devices"),
        (5, "Photography")
    ]
    categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])
    
    # Создаем связи продукт-категория
    product_categories_data = [
        (1, 1),  # Laptop -> Electronics
        (1, 2),  # Laptop -> Computer Accessories
        (2, 1),  # Mouse -> Electronics
        (2, 2),  # Mouse -> Computer Accessories
        (3, 1),  # Keyboard -> Electronics
        (3, 2),  # Keyboard -> Computer Accessories
        (4, 1),  # Monitor -> Electronics
        (4, 2),  # Monitor -> Computer Accessories
        (5, 1),  # Headphones -> Electronics
        (5, 3),  # Headphones -> Audio
        (6, 1),  # Tablet -> Electronics
        (6, 4),  # Tablet -> Mobile Devices
        (7, 1),  # Phone -> Electronics
        (7, 4),  # Phone -> Mobile Devices
        (8, 1),  # Camera -> Electronics
        (8, 5),  # Camera -> Photography
        # Продукты 6, 7, 8 имеют категории, но некоторые продукты могут не иметь категорий
    ]
    product_categories_df = spark.createDataFrame(product_categories_data, ["product_id", "category_id"])
    
    return products_df, categories_df, product_categories_df
