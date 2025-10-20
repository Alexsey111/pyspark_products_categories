"""
Тесты для модуля products_categories.
"""

import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark_app.products_categories import ProductsCategoriesProcessor, create_sample_data


class TestProductsCategoriesProcessor(unittest.TestCase):
    """Тесты для класса ProductsCategoriesProcessor."""
    
    @classmethod
    def setUpClass(cls):
        """Настройка Spark сессии для всех тестов."""
        cls.spark = SparkSession.builder \
            .appName("ProductsCategoriesTest") \
            .master("local[2]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()
        
        # Отключаем логи для чистоты вывода
        cls.spark.sparkContext.setLogLevel("ERROR")
    
    @classmethod
    def tearDownClass(cls):
        """Закрытие Spark сессии после всех тестов."""
        cls.spark.stop()
    
    def setUp(self):
        """Настройка для каждого теста."""
        self.processor = ProductsCategoriesProcessor(self.spark)
    
    def test_get_products_with_categories_basic(self):
        """Тест базовой функциональности получения продуктов с категориями."""
        # Создаем тестовые данные
        products_df, categories_df, product_categories_df = create_sample_data(self.spark)
        
        # Выполняем метод
        result_df = self.processor.get_products_with_categories(
            products_df, categories_df, product_categories_df
        )
        
        # Проверяем структуру результата
        self.assertEqual(len(result_df.columns), 2)
        self.assertIn("product_name", result_df.columns)
        self.assertIn("category_name", result_df.columns)
        
        # Собираем результаты для проверки
        results = result_df.collect()
        
        # Проверяем, что все продукты присутствуют
        product_names = [row.product_name for row in results]
        expected_products = ["Camera", "Headphones", "Keyboard", "Laptop", "Monitor", "Mouse", "Phone", "Tablet"]
        self.assertEqual(sorted(set(product_names)), sorted(expected_products))
        
        # Проверяем, что есть продукты с категориями
        products_with_categories = [row for row in results if row.category_name is not None]
        self.assertGreater(len(products_with_categories), 0)
    
    def test_get_products_without_categories(self):
        """Тест получения продуктов без категорий."""
        # Создаем тестовые данные с продуктом без категорий
        products_data = [
            (1, "Laptop"),
            (2, "Mouse"),
            (3, "Uncategorized Product")  # Продукт без категории
        ]
        products_df = self.spark.createDataFrame(products_data, ["product_id", "product_name"])
        
        categories_data = [
            (1, "Electronics"),
            (2, "Computer Accessories")
        ]
        categories_df = self.spark.createDataFrame(categories_data, ["category_id", "category_name"])
        
        product_categories_data = [
            (1, 1),  # Laptop -> Electronics
            (2, 2),  # Mouse -> Computer Accessories
            # Продукт 3 не имеет категории
        ]
        product_categories_df = self.spark.createDataFrame(
            product_categories_data, ["product_id", "category_id"]
        )
        
        # Тестируем получение продуктов без категорий
        result_df = self.processor.get_products_without_categories(products_df, product_categories_df)
        results = result_df.collect()
        
        # Проверяем результат
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].product_name, "Uncategorized Product")
    
    def test_get_all_products_and_categories(self):
        """Тест получения всех продуктов с категориями."""
        products_df, categories_df, product_categories_df = create_sample_data(self.spark)
        
        result_df = self.processor.get_all_products_and_categories(
            products_df, categories_df, product_categories_df
        )
        
        # Проверяем, что метод возвращает тот же результат, что и get_products_with_categories
        expected_df = self.processor.get_products_with_categories(
            products_df, categories_df, product_categories_df
        )
        
        # Сравниваем результаты
        expected_results = sorted(expected_df.collect(), key=lambda x: (x.product_name, x.category_name or ""))
        actual_results = sorted(result_df.collect(), key=lambda x: (x.product_name, x.category_name or ""))
        
        self.assertEqual(len(expected_results), len(actual_results))
        for expected, actual in zip(expected_results, actual_results):
            self.assertEqual(expected.product_name, actual.product_name)
            self.assertEqual(expected.category_name, actual.category_name)
    
    def test_validation_missing_columns_products(self):
        """Тест валидации при отсутствии колонок в products_df."""
        # Создаем DataFrame без необходимых колонок
        invalid_products_df = self.spark.createDataFrame(
            [("Laptop",)], ["name"]  # Отсутствует product_id
        )
        
        categories_df, _, product_categories_df = create_sample_data(self.spark)[1:]
        
        with self.assertRaises(ValueError) as context:
            self.processor.get_products_with_categories(
                invalid_products_df, categories_df, product_categories_df
            )
        
        self.assertIn("Отсутствуют колонки в products_df", str(context.exception))
    
    def test_validation_missing_columns_categories(self):
        """Тест валидации при отсутствии колонок в categories_df."""
        products_df, _, product_categories_df = create_sample_data(self.spark)[::2]
        
        # Создаем DataFrame без необходимых колонок
        invalid_categories_df = self.spark.createDataFrame(
            [("Electronics",)], ["name"]  # Отсутствует category_id
        )
        
        with self.assertRaises(ValueError) as context:
            self.processor.get_products_with_categories(
                products_df, invalid_categories_df, product_categories_df
            )
        
        self.assertIn("Отсутствуют колонки в categories_df", str(context.exception))
    
    def test_validation_missing_columns_links(self):
        """Тест валидации при отсутствии колонок в product_categories_df."""
        products_df, categories_df, _ = create_sample_data(self.spark)[:2]
        
        # Создаем DataFrame без необходимых колонок
        invalid_links_df = self.spark.createDataFrame(
            [(1, "Electronics")], ["product_id", "category_name"]  # Отсутствует category_id
        )
        
        with self.assertRaises(ValueError) as context:
            self.processor.get_products_with_categories(
                products_df, categories_df, invalid_links_df
            )
        
        self.assertIn("Отсутствуют колонки в product_categories_df", str(context.exception))
    
    def test_empty_dataframes(self):
        """Тест работы с пустыми DataFrame."""
        # Создаем пустые DataFrame с правильной схемой
        empty_products_df = self.spark.createDataFrame([], StructType([
            StructField("product_id", IntegerType(), True),
            StructField("product_name", StringType(), True)
        ]))
        
        empty_categories_df = self.spark.createDataFrame([], StructType([
            StructField("category_id", IntegerType(), True),
            StructField("category_name", StringType(), True)
        ]))
        
        empty_links_df = self.spark.createDataFrame([], StructType([
            StructField("product_id", IntegerType(), True),
            StructField("category_id", IntegerType(), True)
        ]))
        
        # Тестируем с пустыми данными
        result_df = self.processor.get_products_with_categories(
            empty_products_df, empty_categories_df, empty_links_df
        )
        
        self.assertEqual(result_df.count(), 0)
    
    def test_products_with_multiple_categories(self):
        """Тест продуктов с несколькими категориями."""
        # Создаем данные с продуктом, имеющим несколько категорий
        products_data = [(1, "Laptop")]
        products_df = self.spark.createDataFrame(products_data, ["product_id", "product_name"])
        
        categories_data = [
            (1, "Electronics"),
            (2, "Computer Accessories"),
            (3, "Office Equipment")
        ]
        categories_df = self.spark.createDataFrame(categories_data, ["category_id", "category_name"])
        
        product_categories_data = [
            (1, 1),  # Laptop -> Electronics
            (1, 2),  # Laptop -> Computer Accessories
            (1, 3),  # Laptop -> Office Equipment
        ]
        product_categories_df = self.spark.createDataFrame(
            product_categories_data, ["product_id", "category_id"]
        )
        
        result_df = self.processor.get_products_with_categories(
            products_df, categories_df, product_categories_df
        )
        
        results = result_df.collect()
        
        # Проверяем, что Laptop имеет все три категории
        laptop_categories = [row.category_name for row in results if row.product_name == "Laptop"]
        expected_categories = ["Computer Accessories", "Electronics", "Office Equipment"]
        
        self.assertEqual(len(laptop_categories), 3)
        self.assertEqual(sorted(laptop_categories), sorted(expected_categories))
    
    def test_create_sample_data(self):
        """Тест создания примеров данных."""
        products_df, categories_df, product_categories_df = create_sample_data(self.spark)
        
        # Проверяем структуру данных
        self.assertEqual(len(products_df.columns), 2)
        self.assertEqual(len(categories_df.columns), 2)
        self.assertEqual(len(product_categories_df.columns), 2)
        
        # Проверяем количество записей
        self.assertEqual(products_df.count(), 8)
        self.assertEqual(categories_df.count(), 5)
        self.assertGreater(product_categories_df.count(), 0)
        
        # Проверяем наличие данных
        products = [row.product_name for row in products_df.collect()]
        self.assertIn("Laptop", products)
        self.assertIn("Mouse", products)
        
        categories = [row.category_name for row in categories_df.collect()]
        self.assertIn("Electronics", categories)
        self.assertIn("Computer Accessories", categories)


if __name__ == '__main__':
    unittest.main()
